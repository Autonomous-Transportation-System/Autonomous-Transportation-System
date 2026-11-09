"""
L3 Agent 注册表模拟器

在实际系统中，这应该是一个独立的服务（如 etcd, Consul, Zookeeper）
这里我们提供一个 Mock 实现用于开发和测试

支持两种模式：
1. 配置文件模式：从 config/agent_registry.json 读取（生产推荐）
2. Mock 模式：使用代码中定义的默认配置（开发测试）
"""
from __future__ import annotations

import asyncio
import logging
import json
import os
import time
from typing import TYPE_CHECKING, List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

if TYPE_CHECKING:
    from src.graph.distributed_types import AgentInfo, SubWorkflowInfo

logger = logging.getLogger(__name__)


class AgentRegistryClient:
    """L3 Agent 注册表客户端"""
    
    def __init__(
        self, 
        registry_url: str = "http://localhost:8001/registry",
        config_file: str = None,
    ):
        """
        初始化注册表客户端
        
        Args:
            registry_url: 注册表服务的 URL（实际系统中使用）
            config_file: 配置文件路径（支持 JSON 格式）
        """
        self.registry_url = registry_url
        self.config_file = config_file or os.getenv(
            "AGENT_REGISTRY_CONFIG", 
            "config/agent_registry.json"
        )
        self._mock_agents = self._load_agents()

        # ── 子工作流注册表 ────────────────────────────────────────────
        self._sub_workflows: List[SubWorkflowInfo] = self._load_sub_workflows()
        # peer_url → List[SubWorkflowInfo]（来自对等节点的子工作流列表）
        self._peer_sub_workflows: Dict[str, List[SubWorkflowInfo]] = {}

        # ── Gossip 跨节点同步状态 ──────────────────────────────────────
        # peer_url  →  List[AgentInfo]（来自对等节点的 agent 列表）
        self._peer_agents: Dict[str, List[AgentInfo]] = {}
        # peer_url  →  最后同步时间戳（秒）
        self._peer_last_seen: Dict[str, float] = {}
        # gossip 后台 asyncio.Task 引用，防止被 GC
        self._gossip_task: Optional[asyncio.Task] = None

        logger.info(
            f"ℹ️  使用精确匹配 ({len(self._mock_agents)} 个智能体, "
            f"{len(self._sub_workflows)} 个子工作流)"
        )
    
    def _load_agents(self) -> List[AgentInfo]:
        """
        从配置文件或默认配置加载 Agent 列表
        
        优先级：
        1. 配置文件（config/agent_registry.json）
        2. 环境变量指定的配置文件
        3. 默认 Mock 配置
        """
        # 尝试从配置文件加载
        config_path = Path(self.config_file)
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    agents = config_data.get('agents', [])
                    
                    # 只返回启用的 Agent
                    enabled_agents = [
                        agent for agent in agents 
                        if agent.get('enabled', True)
                    ]
                    
                    logger.info(
                        f"✅ 从配置文件加载了 {len(enabled_agents)} 个 Agent: {config_path}"
                    )
                    return enabled_agents
            except Exception as e:
                logger.warning(f"⚠️  配置文件加载失败: {e}，使用默认配置")
        else:
            logger.info(f"⚠️  配置文件不存在: {config_path}，使用默认配置")
        
        # 返回默认 Mock 配置
        return self._init_mock_agents()
    
    def _init_mock_agents(self) -> List[AgentInfo]:
        """
        初始化默认 Mock Agent 列表
        
        用于开发和测试环境
        """
        return [
            {
                "id": "search_agent_001",
                "ip": "127.0.0.1",
                "port": 8080,
                "capability": "search",
                "status": "online",
                "description": "专门用于网络搜索和信息检索的 Agent，支持多引擎搜索"
            },
            {
                "id": "compute_agent_001",
                "ip": "127.0.0.1",
                "port": 8081,
                "capability": "compute",
                "status": "online",
                "description": "专门用于数学计算和数据分析的 Agent，支持 Python/NumPy/Pandas"
            },
            {
                "id": "vision_agent_001",
                "ip": "127.0.0.1",
                "port": 8082,
                "capability": "vision",
                "status": "online",
                "description": "专门用于图像分析和视觉任务的 Agent，支持 OCR、目标检测等"
            },
            {
                "id": "nlp_agent_001",
                "ip": "127.0.0.1",
                "port": 8083,
                "capability": "nlp",
                "status": "online",
                "description": "专门用于自然语言处理的 Agent，支持翻译、摘要、情感分析等"
            },
            {
                "id": "code_agent_001",
                "ip": "127.0.0.1",
                "port": 8084,
                "capability": "code_execution",
                "status": "online",
                "description": "专门用于代码执行的 Agent，支持多种编程语言"
            },
            {
                "id": "web_agent_001",
                "ip": "127.0.0.1",
                "port": 8085,
                "capability": "web_interaction",
                "status": "online",
                "description": "专门用于网页交互的 Agent，支持浏览器自动化操作"
            },
        ]

    def _load_sub_workflows(self) -> List[SubWorkflowInfo]:
        """
        从 config/sub_workflows.json 加载子工作流定义。

        自动补充 owner_ip / owner_port（从 LOCAL_AOE_URL 推断）。
        """
        swf_config = os.getenv("SUB_WORKFLOWS_CONFIG", "config/sub_workflows.json")
        config_path = Path(swf_config)
        if not config_path.exists():
            logger.info(f"⚠️  子工作流配置不存在: {config_path}，跳过加载")
            return []

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # 推断本节点 IP/Port
            import urllib.parse
            local_url = os.getenv("LOCAL_AOE_URL", "http://localhost:8000")
            parsed = urllib.parse.urlparse(local_url)
            owner_ip = parsed.hostname or "127.0.0.1"
            owner_port = parsed.port or 8000

            sub_workflows: List[SubWorkflowInfo] = []
            for swf in data.get("sub_workflows", []):
                if not swf.get("enabled", True):
                    continue
                sub_workflows.append({
                    "id": swf["id"],
                    "capability": swf.get("capability", ""),
                    "description": swf.get("description", ""),
                    "owner_ip": owner_ip,
                    "owner_port": owner_port,
                    "pipeline": swf.get("pipeline", []),
                    "status": "online",
                })

            logger.info(f"✅ 从配置文件加载了 {len(sub_workflows)} 个子工作流: {config_path}")
            return sub_workflows

        except Exception as e:
            logger.warning(f"⚠️  子工作流配置加载失败: {e}")
            return []

    # ──────────────────────────────────────────────────────────────────
    # 子工作流查询
    # ──────────────────────────────────────────────────────────────────

    def get_all_sub_workflows(self) -> List[SubWorkflowInfo]:
        """获取所有子工作流（本地 + peer），以 id 去重（本地优先）"""
        merged: Dict[str, SubWorkflowInfo] = {}
        for peer_list in self._peer_sub_workflows.values():
            for swf in peer_list:
                merged[swf["id"]] = swf
        for swf in self._sub_workflows:
            merged[swf["id"]] = swf
        return list(merged.values())

    def get_sub_workflow_by_id(self, swf_id: str) -> Optional[SubWorkflowInfo]:
        """按 ID 查找子工作流（仅本地）"""
        for swf in self._sub_workflows:
            if swf["id"] == swf_id:
                return swf
        return None

    def get_local_sub_workflows(self) -> List[SubWorkflowInfo]:
        """仅返回本地在线子工作流（用于 Gossip 推送）"""
        return [swf for swf in self._sub_workflows if swf.get("status") == "online"]

    def query_agents(
        self, 
        capability: str = None,
    ) -> List[AgentInfo]:
        """
        查询可用的 Agent（精确匹配）
        
        Args:
            capability: 过滤条件，只返回具有特定能力的 Agent
        
        Returns:
            符合条件的 Agent 列表
        """
        logger.info(f"🔍 使用精确匹配 (capability={capability})")

        agents = self._merge_all_agents()

        # 过滤在线的 Agent
        agents = [a for a in agents if a["status"] == "online"]

        # 如果指定了能力，进行过滤
        if capability:
            agents = [a for a in agents if a["capability"] == capability]

        logger.info(f"✅ 精确匹配返回 {len(agents)} 个智能体")
        return agents
    
    def get_all_agents(self) -> List[AgentInfo]:
        """
        获取所有注册的 Agent（本地 + 所有已同步的 peer agents）

        Returns:
            所有 Agent 的列表
        """
        logger.info("Fetching all agents from registry")
        return self._merge_all_agents()

    # ──────────────────────────────────────────────────────────────────
    # Gossip 跨节点发现
    # ──────────────────────────────────────────────────────────────────

    def _merge_all_agents(self) -> List[AgentInfo]:
        """合并本地 + peer agents，以 id 去重（本地优先）"""
        merged: Dict[str, AgentInfo] = {}
        # 先放 peer，再放本地（本地覆盖 peer 同 id 记录）
        for peer_list in self._peer_agents.values():
            for a in peer_list:
                merged[a["id"]] = a
        for a in self._mock_agents:
            merged[a["id"]] = a
        return list(merged.values())

    def get_local_agents(self) -> List[AgentInfo]:
        """仅返回本地 online agents，用于向 peer 推送"""
        return [a for a in self._mock_agents if a.get("status") == "online"]

    def sync_from_peer(
        self,
        peer_url: str,
        agents: List[AgentInfo],
        sub_workflows: Optional[List[SubWorkflowInfo]] = None,
    ) -> int:
        """
        接收来自 peer 节点的 agent 列表和子工作流列表，更新内部 peer 缓存。

        Args:
            peer_url:       来源节点 URL
            agents:         该节点的 online agent 列表
            sub_workflows:  该节点的 online 子工作流列表（可选，向后兼容）

        Returns:
            合并后 peer agents 数量
        """
        self._peer_agents[peer_url] = agents
        self._peer_last_seen[peer_url] = time.time()
        if sub_workflows is not None:
            self._peer_sub_workflows[peer_url] = sub_workflows
            logger.info(
                f"[ARDC Gossip] 收到来自 {peer_url} 的同步，"
                f"{len(agents)} 个 agents, {len(sub_workflows)} 个子工作流"
            )
        else:
            logger.info(
                f"[ARDC Gossip] 收到来自 {peer_url} 的同步，{len(agents)} 个 agents"
            )
        return sum(len(v) for v in self._peer_agents.values())

    def prune_stale_peers(self, ttl_seconds: int = 120):
        """
        清理超过 TTL 未更新的 peer 缓存，避免幽灵 agent 残留。

        Args:
            ttl_seconds: 过期阈值，默认 120 秒
        """
        now = time.time()
        stale = [
            url for url, ts in self._peer_last_seen.items()
            if now - ts > ttl_seconds
        ]
        for url in stale:
            del self._peer_agents[url]
            del self._peer_last_seen[url]
            self._peer_sub_workflows.pop(url, None)
            logger.info(f"[ARDC Gossip] 清理过期 peer: {url}")

    async def push_to_peer(self, peer_url: str, local_url: str) -> bool:
        """
        向指定 peer 推送本地 agent 列表（HTTP Push Gossip）。
        失败静默忽略，不影响本节点正常运行。

        Args:
            peer_url:  目标 peer 的 HTTP 地址，如 "http://192.168.1.20:8000"
            local_url: 本节点地址（让对等方知道来源，用于反向 sync）

        Returns:
            True 表示推送成功
        """
        import httpx
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(
                    f"{peer_url}/registry/sync",
                    json={
                        "source_url": local_url,
                        "agents": self.get_local_agents(),
                        "sub_workflows": self.get_local_sub_workflows(),
                    },
                )
                resp.raise_for_status()
                logger.info(
                    f"[ARDC Gossip] 推送到 {peer_url} 成功: "
                    f"{resp.json().get('merged_count', '?')} peer agents"
                )
                return True
        except Exception as e:
            logger.warning(f"[ARDC Gossip] 推送到 {peer_url} 失败（非关键）: {e}")
            return False

    async def start_gossip_background(
        self,
        peer_urls: List[str],
        local_url: str,
        interval: int = 30,
        ttl_seconds: int = 120,
    ):
        """
        启动 gossip 后台 asyncio 任务：
        每 interval 秒向所有 peer 推送本地 agent 列表，并清理过期 peer 缓存。

        通常在应用 startup 事件中调用。

        Args:
            peer_urls:   对等节点 URL 列表（从环境变量 PEER_AOE_URLS 读取）
            local_url:   本节点地址
            interval:    推送间隔（秒），默认 30
            ttl_seconds: peer 缓存 TTL（秒），默认 120
        """
        if not peer_urls:
            logger.info("[ARDC Gossip] 未配置 PEER_AOE_URLS，gossip 不启动")
            return

        async def _loop():
            logger.info(
                f"[ARDC Gossip] 后台任务启动，peers={peer_urls}, "
                f"interval={interval}s"
            )
            while True:
                self.prune_stale_peers(ttl_seconds=ttl_seconds)
                for peer_url in peer_urls:
                    await self.push_to_peer(peer_url, local_url)
                await asyncio.sleep(interval)

        self._gossip_task = asyncio.create_task(_loop())
        logger.info("[ARDC Gossip] 后台任务已创建")
    
    def get_agent_by_id(self, agent_id: str) -> AgentInfo:
        """
        根据 ID 获取特定 Agent
        
        Args:
            agent_id: Agent 的唯一标识符
        
        Returns:
            Agent 信息，如果不存在则返回 None
        """
        for agent in self._mock_agents:
            if agent["id"] == agent_id:
                return agent
        return None
    
    def update_agent_status(self, agent_id: str, status: str):
        """
        更新 Agent 状态（Mock 实现）

        Args:
            agent_id: Agent ID
            status: 新状态
        """
        logger.info(f"Updating agent {agent_id} status to {status}")
        for agent in self._mock_agents:
            if agent["id"] == agent_id:
                agent["status"] = status
                break

    def register_agent(
        self,
        agent_id: str,
        ip: str,
        port: int,
        capability: str,
        description: str = "",
        status: str = "online",
    ):
        """
        注册或更新一个 Agent 条目（由 ASD subprocess 部署后调用）。

        若 agent_id 已存在则更新 ip/port/status；
        否则追加新记录。

        Args:
            agent_id:    Agent 逻辑 ID
            ip:          实际监听 IP
            port:        实际监听端口
            capability:  能力标签（如 'search'）
            description: 可选描述
            status:      初始状态，默认 'online'
        """
        for agent in self._mock_agents:
            if agent["id"] == agent_id:
                agent["ip"] = ip
                agent["port"] = port
                agent["capability"] = capability
                agent["status"] = status
                if description:
                    agent["description"] = description
                logger.info(
                    f"[ARDC] 更新 agent: id={agent_id}, {ip}:{port}, cap={capability}"
                )
                return
        # 新增
        self._mock_agents.append({
            "id": agent_id,
            "ip": ip,
            "port": port,
            "capability": capability,
            "status": status,
            "description": description or f"subprocess 部署的 {capability} agent",
        })
        logger.info(
            f"[ARDC] 注册新 agent: id={agent_id}, {ip}:{port}, cap={capability}"
        )
    
    def get_registry_summary(self) -> Dict[str, Any]:
        """
        获取注册表摘要信息
        
        Returns:
            包含统计信息的字典
        """
        total = len(self._mock_agents)
        online = len([a for a in self._mock_agents if a["status"] == "online"])
        capabilities = list(set(a["capability"] for a in self._mock_agents))
        
        summary = {
            "total_agents": total,
            "online_agents": online,
            "available_capabilities": capabilities,
            "last_update": datetime.now().isoformat(),
            "retrieval_mode": "Exact Match"
        }
        
        return summary


# 全局单例实例
_registry_client_instance = None


def get_registry_client() -> AgentRegistryClient:
    """
    获取注册表客户端的全局单例
    
    Returns:
        AgentRegistryClient 实例
    """
    global _registry_client_instance
    if _registry_client_instance is None:
        _registry_client_instance = AgentRegistryClient()
    return _registry_client_instance
