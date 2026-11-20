# QF-05：关账备份 v2 与 v1 兼容

> 后续真实 celld 与兼容部署已完成，见 [部署验收](period-backup-v2-deployment.md)。本文保留格式内核阶段记录。

日期：2026-11-18。QF-05 继续 in_progress。

本批补齐启用关账服务前的备份前置条件。AuthorityArchiveStore 在发现关账身份、单据、请求回执或调整数据时，自动生成 schemaVersion=2；空 period 表不改变原有 v1 格式。v2 仍使用现有签名信封目的，schemaVersion 受 HMAC 保护，表清单按版本严格校验。

新增白名单包含 tenant_settlement_identity、tenant_period_closes、tenant_period_close_requests、tenant_period_adjustments。归档和离线恢复验证关账摘要/冻结 basis、租户身份、回执引用，以及调整关联的原关账和用量账目；不会执行关账、释放预算或启动服务。

验证结果：严格控制面检查与187项测试通过，325模块构建通过。新增原生SQLite测试创建隔离关账单和晚到调整，导出v2并恢复到新离线数据库，单据及调整保持一致；同一测试保留并恢复此前的v1快照。另使用升级后的CLI，成功独立恢复此前开发环境保存的三份实际v1文件，源摘要与原验收结果一致，临时目标随后清理。

本批未部署新格式，也未在开发环境创建费率、执行任务或最终账单。生产执行清单认证、实际成本分摊/预算释放、关账API、自动晚到钩子、人工调整及真实服务故障验收仍未完成。新的v2还需实际celld服务与部署恢复验收，不能把本次SQLite/文件兼容验证视作关账服务上线。
