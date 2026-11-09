"""
应用管理器 (APPM - Application Manager)

应用管理层的核心聚合组件，负责：
- 聚合 AW（智能体仓库）和 ALRE（应用逻辑执行引擎）
- 提供应用全生命周期管理接口：安装/启动/停止/卸载
- 维护 AppInfo 状态（idle/starting/running/stopping/stopped）

对应接口文档：
- §1 安装/卸载应用（APPM 部分）
- §2 启动应用（APPM 部分）
- §3 停止应用（APPM 部分）
"""
import json
import logging
import os
import tempfile
import uuid
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Optional, get_args

from src.app.models import AgentImage, AppInfo, AppStatus, GuidanceFile

APPS_STORE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "apps_store.json")

logger = logging.getLogger(__name__)
_UNSET = object()


class AppManager:
    """
    应用管理器（APPM）

    职责：
    1. install()    — 安装镜像（调 AW）+ 存储指导文件（调 ALRE）
    2. uninstall()  — 删除指导文件（调 ALRE）+ 卸载镜像（调 AW）
    3. start()      — 启动编排工作流（调 ALRE.start_app）
    4. stop()       — 停止工作流（调 ALRE.stop_app）
    5. list_apps()  — 返回所有 AppInfo
    6. get_app()    — 查询单个 AppInfo
    """

    def __init__(self, store_path=None, *, execution_backend=None):
        """Optionally inject a store and an ALRE-compatible execution backend.

        The default backend and the ordinary/periodic workflow APIs are retained.
        Traffic bridges use install/update_state without invoking start_app.
        """
        # app_id → AppInfo
        self._apps: Dict[str, AppInfo] = {}
        self._execution_backend = execution_backend
        self._execution_backends: Dict[str, object] = {}
        self._store_path = os.path.abspath(
            APPS_STORE_PATH if store_path is None else os.fspath(store_path)
        )
        os.makedirs(os.path.dirname(self._store_path), exist_ok=True)
        self._load_from_disk()
        logger.info("AppManager (APPM) 初始化完成")

    def _load_from_disk(self):
        """从磁盘恢复应用列表"""
        if not os.path.exists(self._store_path):
            return
        try:
            with open(self._store_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            reset = False
            for app_id, app_dict in data.items():
                gf_dict = app_dict.pop("guidance_file", None)
                guidance_file = None
                if gf_dict:
                    guidance_file = GuidanceFile(**gf_dict)
                app = AppInfo(**app_dict, guidance_file=guidance_file)
                # 重启后将运行中/启动中状态重置为 stopped
                if app.status in ("running", "starting", "stopping", "scheduled"):
                    app.status = "stopped"
                    app.workflow_handle = None
                    app.app_interface_url = None
                    reset = True
                    if guidance_file and guidance_file.metadata.get("execution_backend") in ("traffic", "traffic_joint"):
                        run = guidance_file.metadata.setdefault("traffic_run", {})
                        run.update(last_run_status="interrupted", active_workflow_id=None,
                                   finished_at=datetime.utcnow().isoformat())
                self._apps[app_id] = app
                # 同步恢复 ALRE 的指导文件内存字典
                if guidance_file:
                    engine = self._get_engine()
                    engine.install_app_logic(guidance_file)
            if reset:
                self._save_to_disk()
            logger.info(f"[APPM] 从磁盘恢复 {len(self._apps)} 个应用")
        except Exception as e:
            logger.warning(f"[APPM] 恢复应用列表失败: {e}")

    def _save_to_disk(self):
        """将应用列表持久化到磁盘"""
        temporary = None
        try:
            data = {app_id: app.to_dict() for app_id, app in self._apps.items()}
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=os.path.dirname(self._store_path),
                prefix=".apps-", suffix=".json", delete=False,
            ) as f:
                temporary = f.name
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temporary, self._store_path)
        except Exception as e:
            logger.warning(f"[APPM] 保存应用列表失败: {e}")
            raise
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)

    def update_state(
        self, app_id: str, status: AppStatus, *, expected_handle=_UNSET,
        workflow_handle=_UNSET, app_interface_url=_UNSET, error=_UNSET,
        metadata: Optional[dict] = None,
    ) -> bool:
        """Persist a lifecycle transition, optionally comparing the current handle.

        expected_handle=None explicitly requires no current execution. Metadata
        lives in the existing GuidanceFile and survives AppInfo serialization.
        A stale completion returns False without touching any persisted field.
        """
        if status not in get_args(AppStatus):
            raise ValueError(f"Unknown application status: {status}")
        app = self._apps.get(app_id)
        if app is None or (expected_handle is not _UNSET and app.workflow_handle != expected_handle):
            return False
        if metadata is not None and app.guidance_file is None:
            raise ValueError("Application metadata requires a GuidanceFile")
        previous = deepcopy(app)
        app.update_status(status)
        if workflow_handle is not _UNSET:
            app.workflow_handle = workflow_handle
        if app_interface_url is not _UNSET:
            app.app_interface_url = app_interface_url
        if error is not _UNSET:
            app.error_message = error
        if metadata is not None:
            app.guidance_file.metadata.update(deepcopy(metadata))
        try:
            self._save_to_disk()
        except Exception:
            self._apps[app_id] = previous
            raise
        return True

    def register_execution_backend(self, kind: str, backend) -> None:
        """Register an application backend whose start/stop return bridge dictionaries.

        Configuration storage remains ALRE-compatible. Registration never loads
        models and cannot replace another live backend of the same kind.
        """
        if not kind or not callable(getattr(backend, "start", None)) or not callable(getattr(backend, "stop", None)):
            raise ValueError("A named backend requires start and stop methods")
        existing = self._execution_backends.get(kind)
        if existing is not None and existing is not backend:
            raise ValueError(f"Execution backend already registered: {kind}")
        self._execution_backends[kind] = backend

    def unregister_execution_backend(self, kind: str, backend) -> bool:
        """Remove only the expected backend, after its applications have stopped."""
        if self._execution_backends.get(kind) is not backend:
            return False
        if any(app.status in ("starting", "running", "stopping") and app.guidance_file and
               app.guidance_file.metadata.get("execution_backend") == kind for app in self._apps.values()):
            raise RuntimeError("Cannot unregister a backend with active applications")
        del self._execution_backends[kind]
        return True

    def _registered_backend(self, app):
        kind = app.guidance_file.metadata.get("execution_backend") if app.guidance_file else None
        if kind is None:
            return None
        backend = self._execution_backends.get(kind)
        if backend is None:
            raise RuntimeError(f"Execution backend is unavailable: {kind}")
        return backend

    # ------------------------------------------------------------------
    # 安装 / 卸载
    # ------------------------------------------------------------------

    def install(
        self,
        name: str,
        guidance_file: GuidanceFile,
        images: Optional[List[AgentImage]] = None,
        expose_external: bool = False,
    ) -> AppInfo:
        """
        安装应用

        流程（参考接口文档 §1）：
        1. 可选：调用 AW 安装 Agent 镜像并注册到 ARDC
        2. 调用 ALRE 保存编排指导文件
        3. 创建并保存 AppInfo

        Args:
            name:             应用名称
            guidance_file:    编排指导文件（任务描述、约束等）
            images:           包含的 Agent 镜像列表（可选）
            expose_external:  是否外部可见

        Returns:
            AppInfo
        """
        # 确保 guidance_file.app_id 一致
        if not guidance_file.app_id:
            guidance_file.app_id = f"app_{uuid.uuid4().hex[:8]}"

        app = AppInfo.create(name=name, guidance_file=guidance_file)
        app.app_id = guidance_file.app_id  # 与 guidance_file 使用同一 app_id

        # 1. 安装镜像到 AW（可选）
        if images:
            warehouse = self._get_warehouse()
            for img in images:
                warehouse.install_agent(img, expose_external=expose_external)
                app.image_ids.append(img.image_id)

        # 2. 保存指导文件到 ALRE
        engine = self._get_engine()
        engine.install_app_logic(guidance_file)

        # 3. 保存应用信息
        self._apps[app.app_id] = app
        self._save_to_disk()

        logger.info(
            f"[APPM] ✅ 安装完成: app_id={app.app_id}, name={name}, "
            f"images={len(images or [])}"
        )
        return app

    def uninstall(self, app_id: str) -> bool:
        """
        卸载应用

        流程（参考接口文档 §1）：
        1. 若正在运行，先停止（同步版本：标记停止意图）
        2. 调用 ALRE 删除指导文件
        3. 调用 AW 卸载镜像
        4. 删除 AppInfo

        Args:
            app_id: 应用 ID

        Returns:
            True 表示成功，False 表示未找到
        """
        app = self._apps.get(app_id)
        if not app:
            logger.warning(f"[APPM] uninstall: app_id={app_id} 未找到")
            return False

        if app.guidance_file and app.guidance_file.metadata.get("execution_backend") and app.status in ("starting", "running", "stopping"):
            raise RuntimeError("Stop the execution backend before uninstalling this application")

        if app.status == "running":
            logger.warning(
                f"[APPM] uninstall: app_id={app_id} 正在运行，建议先调用 stop()"
            )

        # 删除 ALRE 指导文件
        engine = self._get_engine()
        engine.uninstall_app_logic(app_id)

        # 卸载 AW 中的镜像
        if app.image_ids:
            warehouse = self._get_warehouse()
            for image_id in app.image_ids:
                warehouse.uninstall_agent(image_id)

        # 删除应用记录
        del self._apps[app_id]
        self._save_to_disk()
        logger.info(f"[APPM] ✅ 卸载完成: app_id={app_id}")
        return True

    # ------------------------------------------------------------------
    # 启动 / 停止
    # ------------------------------------------------------------------

    async def start(self, app_id: str) -> Optional[str]:
        """
        启动应用

        流程（参考接口文档 §2）：
        1. 更新状态为 starting
        2. 调用 ALRE.start_app()
        3. 更新状态为 running，填充 workflow_handle

        Args:
            app_id: 应用 ID

        Returns:
            workflow_handle（str），失败返回 None
        """
        app = self._apps.get(app_id)
        if not app:
            logger.error(f"[APPM] start: app_id={app_id} 未找到")
            return None

        backend = self._registered_backend(app)
        if backend is not None:
            result = await backend.start(app_id)
            return result["workflow_id"]

        if app.status == "running":
            logger.warning(f"[APPM] start: app_id={app_id} 已在运行中")
            return app.workflow_handle

        app.update_status("starting")

        try:
            engine = self._get_engine()
            handle = await engine.start_app(app_id)

            if handle:
                app.workflow_handle = handle
                app.app_interface_url = f"/api/apps/{app_id}/interface"
                app.update_status("running")
                logger.info(f"[APPM] ✅ 启动成功: app_id={app_id}, handle={handle}")
            else:
                app.update_status("error", "ALRE 未能启动工作流")
                logger.error(f"[APPM] 启动失败: app_id={app_id}")

            self._save_to_disk()
            return handle

        except Exception as e:
            app.update_status("error", str(e))
            logger.error(f"[APPM] 启动异常: app_id={app_id}, error={e}")
            return None

    async def stop(self, app_id: str) -> bool:
        """
        停止应用

        流程（参考接口文档 §3）：
        1. 更新状态为 stopping
        2. 调用 ALRE.stop_app()
        3. 更新状态为 stopped

        Args:
            app_id: 应用 ID

        Returns:
            True 表示成功停止，False 表示失败
        """
        app = self._apps.get(app_id)
        if not app:
            logger.warning(f"[APPM] stop: app_id={app_id} 未找到")
            return False

        backend = self._registered_backend(app)
        if backend is not None:
            result = await backend.stop(app_id)
            # Legacy API renders stopped unconditionally. Preserve its bool
            # contract, and wait for real termination before it can render that.
            if not result["done"]:
                await backend.wait(app_id)
            return self._apps[app_id].status == "stopped"

        if app.status not in ("running", "starting"):
            logger.warning(
                f"[APPM] stop: app_id={app_id} 当前状态 {app.status}，无需停止"
            )
            return False

        app.update_status("stopping")

        try:
            engine = self._get_engine()
            success = await engine.stop_app(app_id)

            if success:
                app.workflow_handle = None
                app.app_interface_url = None
                app.update_status("stopped")
                logger.info(f"[APPM] ✅ 停止成功: app_id={app_id}")
            else:
                app.update_status("stopped")  # 无论如何标记为 stopped
                logger.warning(f"[APPM] stop: ALRE 未找到运行工作流，标记为 stopped")

            self._save_to_disk()
            return True

        except Exception as e:
            app.update_status("error", str(e))
            logger.error(f"[APPM] 停止异常: app_id={app_id}, error={e}")
            return False

    # ------------------------------------------------------------------
    # 周期调度
    # ------------------------------------------------------------------

    async def start_schedule(self, app_id: str) -> bool:
        """
        启动应用的周期调度

        从 GuidanceFile.constraints 读取调度配置：
        - schedule_interval_seconds (必填)
        - schedule_max_parallel (默认 5)
        - schedule_max_history (默认 100)

        Args:
            app_id: 应用 ID

        Returns:
            True 表示成功启动
        """
        app = self._apps.get(app_id)
        if not app:
            logger.error(f"[APPM] start_schedule: app_id={app_id} 未找到")
            return False

        if app.guidance_file and app.guidance_file.metadata.get("execution_backend") in ("traffic", "traffic_joint"):
            logger.warning("[APPM] Traffic apps must use the joint traffic runtime")
            return False

        if app.status == "scheduled":
            logger.warning(f"[APPM] start_schedule: app_id={app_id} 已在调度中")
            return False

        if not app.guidance_file:
            logger.error(f"[APPM] start_schedule: app_id={app_id} 无指导文件")
            return False

        constraints = app.guidance_file.constraints
        interval = constraints.get("schedule_interval_seconds")
        if not interval or int(interval) < 1:
            logger.error(
                f"[APPM] start_schedule: app_id={app_id} "
                f"未配置 schedule_interval_seconds 或值无效"
            )
            return False

        max_parallel = int(constraints.get("schedule_max_parallel", 5))
        max_history = int(constraints.get("schedule_max_history", 100))

        scheduler = self._get_scheduler()
        success = await scheduler.start_schedule(
            app_id, int(interval), max_parallel, max_history
        )
        if success:
            app.update_status("scheduled")
            self._save_to_disk()
            logger.info(
                f"[APPM] 周期调度已启动: app_id={app_id}, "
                f"interval={interval}s"
            )
        return success

    async def stop_schedule(self, app_id: str) -> bool:
        """
        停止应用的周期调度

        活跃的工作流实例继续运行直到完成。

        Args:
            app_id: 应用 ID

        Returns:
            True 表示成功停止
        """
        app = self._apps.get(app_id)
        if not app:
            logger.warning(f"[APPM] stop_schedule: app_id={app_id} 未找到")
            return False

        if app.status != "scheduled":
            logger.warning(
                f"[APPM] stop_schedule: app_id={app_id} "
                f"当前状态 {app.status}，非 scheduled"
            )
            return False

        scheduler = self._get_scheduler()
        success = await scheduler.stop_schedule(app_id)
        if success:
            app.update_status("stopped")
            self._save_to_disk()
            logger.info(f"[APPM] 周期调度已停止: app_id={app_id}")
        return success

    async def restore_schedules(self):
        """
        恢复之前处于 scheduled 状态的应用的周期调度。

        在 FastAPI startup 事件中调用。仅恢复配置了
        schedule_auto_restart: true 的应用。
        """
        restored = 0
        for app in self._apps.values():
            if app.status != "stopped" or not app.guidance_file:
                continue
            constraints = app.guidance_file.constraints
            auto_restart = constraints.get("schedule_auto_restart", False)
            interval = constraints.get("schedule_interval_seconds")
            if auto_restart and interval and int(interval) >= 1:
                success = await self.start_schedule(app.app_id)
                if success:
                    restored += 1
        if restored:
            logger.info(f"[APPM] 自动恢复了 {restored} 个周期调度")

    # ------------------------------------------------------------------
    # 查询接口
    # ------------------------------------------------------------------

    def get_app(self, app_id: str) -> Optional[AppInfo]:
        """按 app_id 查询应用信息"""
        return self._apps.get(app_id)

    def list_apps(self) -> List[AppInfo]:
        """列出所有应用"""
        return list(self._apps.values())

    def list_running_apps(self) -> List[AppInfo]:
        """列出所有运行中的应用"""
        return [a for a in self._apps.values() if a.status == "running"]

    def update(
        self,
        app_id: str,
        name: Optional[str] = None,
        task_description: Optional[str] = None,
        skills_content: Optional[str] = None,
        orchestration_mode: Optional[str] = None,
        constraints: Optional[dict] = None,
    ) -> Optional[AppInfo]:
        """
        更新应用配置，无需重新安装

        不允许对运行中的应用修改 skills_content 或 orchestration_mode（因为
        这会改变工作流行为，需要重启）。task_description 和 name 可随时修改。

        Returns:
            更新后的 AppInfo，app_id 不存在时返回 None
        """
        app = self._apps.get(app_id)
        if not app:
            logger.warning(f"[APPM] update: app_id={app_id} 未找到")
            return None

        if name is not None:
            app.name = name

        if app.guidance_file:
            if task_description is not None:
                app.guidance_file.task_description = task_description
            if skills_content is not None:
                app.guidance_file.skills_content = skills_content
                # 同步更新 ALRE 的缓存
                engine = self._get_engine()
                engine.install_app_logic(app.guidance_file)
            if orchestration_mode is not None:
                app.guidance_file.orchestration_mode = orchestration_mode
            if constraints is not None:
                app.guidance_file.constraints.update(constraints)

        app.updated_at = __import__("datetime").datetime.utcnow().isoformat()
        self._save_to_disk()
        logger.info(f"[APPM] ✅ 更新完成: app_id={app_id}")
        return app

    # ------------------------------------------------------------------
    # 内部帮助
    # ------------------------------------------------------------------

    def _get_warehouse(self):
        from src.app.agent_warehouse import get_agent_warehouse
        return get_agent_warehouse()

    def _get_engine(self):
        if self._execution_backend is not None:
            return self._execution_backend
        from src.app.app_logic_engine import get_app_logic_engine
        return get_app_logic_engine()

    def _get_scheduler(self):
        from src.service.workflow_scheduler import get_workflow_scheduler
        return get_workflow_scheduler()


# ======================================================================
# 单例访问
# ======================================================================
_manager_instance: Optional[AppManager] = None


def get_app_manager() -> AppManager:
    """获取全局 AppManager 单例"""
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = AppManager()
    return _manager_instance
