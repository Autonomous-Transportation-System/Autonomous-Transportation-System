# 并行实施模块接口

共享类型：`src.service.traffic_types`的Candidate、PlanNode、WorkflowPlan、Assignment。节点依赖使用同一计划内node_id，调度器内部使用`workflow_id:node_id`。时间为单调秒，非负且按事件不递减；deadline是绝对单调时间。

## 目录与编译（catalog worker）

`TrafficCatalog()`：register_implementation(dict)、register_instance(dict)、candidates(capability_id, contract=None, allowed_instances=None)->tuple[Candidate]、set_health(instance_id, healthy, reason=None)、record_execution(instance_id, capability_id, duration, success)、snapshot()->dict。

implementation包含implementation_id、version、capabilities（capability_id到资源/profile/contract描述）。instance包含instance_id、implementation_id、endpoint、max_concurrency、healthy、base_memory_mb。目录只描述与选择，不加载模型或执行工作。

`TrafficWorkflowCompiler(catalog, model_version, normalization_sha256)`；compile(request:dict, app_id="", workflow_id=None, now=0)->WorkflowPlan。请求模板flow、analysis、od_hotspots；target_frames、deadline_seconds、priority、max_attempts、enable_reuse、enable_contracts、enable_recovery及可选contract_overrides。非法语义拒绝，关闭契约消融只用于受控验证，不允许改变实际张量处理规则。

能力：history.prepare、traffic.predict、validate.state、validate.flow、validate.od、analysis.congestion、analysis.od_hotspots、result.aggregate。输出键state/flow/od。flow只校验flow；analysis校验state+flow并运行规则分析；od_hotspots只校验od并运行热点分析。

## 联合调度（scheduler worker）

`JointScheduler(cpu_budget, memory_budget, max_concurrency, policy="joint", aging_interval=30, enable_recovery=True)`；submit(plan, now)、tick(now)->list[Assignment]、start(task_id, now)、finish(task_id, now, call_status="success", quality_status="valid", *, result=None, error=None)、start_failed(task_id, now, *, error=None)、cancel(workflow_id, now, *, reason=None)、set_instance_health(instance_id, healthy, now)、snapshot(workflow_id=None, include_results=False)->dict。

策略fcfs/drc/edf/heft/joint。共享public ResourcePool，保留旧traffic_scheduler.py不改。snapshot包含tasks（全局task_id到状态dict，含workflow_id/node_id/instance_id/attempt）、workflows（status/done/submitted_at/deadline/finished_at）、resources/resource_events/reservations/peaks。失败后释放真实终态的资源，再按max_attempts重选候选；运行中取消/超时不提前释放。动态health状态优先于候选快照。执行归executor所有。

## 运行时（runtime worker）

`JointTrafficRuntime(experiment_root=None, *, cpu_budget=4, memory_budget=2048, max_concurrency=4, policy="joint", instance_count=2, enable_reuse=True, enable_recovery=True)`；async initialize()、submit(request:dict, app_id="")->workflow_id、async wait(workflow_id)->dict、async run(request:dict, app_id="")->dict、cancel(workflow_id, reason=None)、snapshot(workflow_id=None)->dict、async shutdown()。

公开catalog/compiler/scheduler及instance状态、真实推理计数/profile、缓存命中及资源实测。submit须在initialize完成后调用并同步返回ID；run负责初始化。运行数据在runtime中保存，scheduler只保存结果状态。执行prepare、predict、按需validate、规则analysis和aggregate；不同实例加载真实B1服务。缓存/合并按模型、数据、窗口、归一化版本确定完整键，有界且失败不缓存。同步Future真实终态负责finish和release，HTTP等待取消不能破坏运行owner。

## 管理与API（integration worker）

`TrafficManagementBridge(runtime, manager=None, registry=None, qos=None)`：create(name, request)->dict、list_apps()->list、get_app(app_id)->dict、async start(app_id)->dict（返回app_id/workflow_id）、async stop(app_id)->dict、async shutdown()、snapshot()->dict。应用配置使用既有GuidanceFile，状态更新持久化且校验handle，调用QoS/注册接口，保护mock不改。新API工厂`create_management_router(bridge)`由父代理挂入standalone，路由前缀`/traffic/platform`，保留旧`/traffic/*`。

各worker只改分配文件，其他人并行编辑，不能覆盖或回退他人变更。跨模块变更先通知父代理。

## 安全边界补充（架构审查后冻结）

- 初始化并发调用共用一次实际初始化，全部组件/实例就绪后才ready；未ready或已关闭的submit拒绝且不留下部分记录。
- parameters采用显式输入绑定：`{"inputs":{"prediction":{"node_id":"predict","output":"flow"}}}`。引用必须是依赖中的生产者，并检查端口。prepare输出`inputs`（标准化三模态）及schema_trace；predict输出state/flow/od各自prediction对象；validate输出对应任务键；analysis输出analysis；aggregate按显式绑定收集。predict参数输入键为model_inputs，绑定prepare.inputs。Compiler对计划字典防御性复制，提交也复制，结果缓存不暴露可写共享数组。
- runtime使用同一monotonic now调用compile与submit。deadline为软时效目标（记录miss并用于选择）；PlanNode.timeout_seconds为执行期限，到期请求取消并记录timeout，真实工作结束后才回收。资源等待/期限检查有定时唤醒，不仅等待Future。
- Scheduler memory_budget是扣除常驻额度后的动态任务额度；runtime snapshot另外报告总预算、实例/数据/缓存常驻额度及可用任务额度。Candidate.memory_mb为增量预留。缓存有字节上限，实例退出前不返还其常驻额度；实测RSS与声明额度独立报告。
- Scheduler唯一负责重试。runtime回调携带Assignment的task_id/attempt/reservation_id并过滤迟到或重复回调。取消不可重试，失败/质量无效及确认未启动失败按有限max_attempts处理。
- singleflight物理推理Future独立于消费者workflow。取消一个消费者不终止另一个消费者的计算，任何消费者仍需各自做输出校验；缓存仅存成功实际结果且保留完整语义键。推理次数仅计真实forward。跟随者的预留如用于其请求缓冲/等待，应如实记为请求增量配额，不重复记常驻模型内存；优化收益以真实forward次数和时间评估。

## 最终绑定及管理补充

编译结果在每个节点parameters保存递归冻结的`candidate_adapter_schemas[target_instance_id][input_name][source_instance_id][field] = {"source": schemaDict, "target": schemaDict}`。无需张量转换的绑定仍保存到source_instance_id一级，值为空映射。runtime通过已完成上游assignment和当前实际target实例读取这一快照，使用原traffic_contracts.adapter执行合法转换，重建预测metadata并保存实际trace；identity转换跳过。model_inputs逐模态处理，预测对象的raw/normalized张量保持各自schema一致。训练统计及源数组不修改。完整张量契约影响语义复用键，实例物理ID不破坏跨模板复用。缓存只接纳完整state/flow/od合格结果；契约无效记success+invalid，执行异常记failed+not_evaluated。

管理增加`POST /templates`与动态`GET /templates`、`POST /instances/{id}/health`和`GET /workflows/{id}?wait=true`。template严格为非空字符串；自定义图按真实capability填写Guidance agents_required。QoS仅对本bridge拥有的实例反馈，默认冷却60秒，告警隔离影响后续选择并保留qos_actions。显式健康恢复不回收已运行工作的资源。等待查询通过runtime.wait，HTTP waiter取消不取消执行owner。

工作流成功终态为`status="completed"`，任务调用成功仍为`call_status="success"`。A2A外层将completed映射为payload.status=success；浏览器和管理查询保留completed。失败或取消不映射为成功。Compiler允许不含result.aggregate、由终端生产者提供requested_outputs的模板，runtime必须按同一声明收集其最终业务输出。

单个aggregate允许绑定全请求输出的子集及可选analysis。注册时要求所有终端导出端口的并集覆盖requested_outputs，运行时按实际终端生产者收集；不同生产者导出同名结果属于歧义，准入前拒绝。同一生产者经重复终端导出时只保留一份且检查内容一致。API已验证分工aggregate和aggregate/直接终端混合形式。
