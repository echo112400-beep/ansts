# M3 开发试点费率：待确认

仅在开发环境激活给 `m3-pilot`，不改其他租户。保留每笔 5 信用单位的预估。旧试点记录缺可信用量，保持待核算。

创建请求：`POST /v1/admin/finops/rate-cards`，幂等键 `m3-dev-container-card-v1`。

```json
{
  "expectedVersion": 0,
  "reason": "M3 development-only container metering verification",
  "definition": {
    "schemaVersion": 1,
    "unit": "credit-subunit",
    "unitVersion": 1,
    "rounding": "CEIL",
    "scope": {"region": "dev", "poolId": null},
    "resources": {
      "cpuNanoCoreMilliseconds": {"numerator": "1", "denominator": "10000000000000"},
      "memoryMiBMilliseconds": {"numerator": "1", "denominator": "10240000"},
      "slotMilliseconds": {"numerator": "1", "denominator": "10000"}
    },
    "models": []
  }
}
```

激活请求：`PUT /v1/admin/tenants/m3-pilot/rate-card`，幂等键 `m3-dev-container-activation-v1`，expectedVersion=0，rateCardVersion 使用创建返回的版本。创建前重新确认目录版本和租户激活版本。以上为提案，未发送。

独立核查：双 Gateway 对 2026-10 月结预览均返回 incomplete、nonEmpty=false、HISTORICAL_COVERAGE_UNAVAILABLE。2026-11 仍未结束，预览另报告 Body 尝试历史、费率及价格快照覆盖不足。新试点真实计价无法补齐旧历史。
