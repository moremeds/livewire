# SE-07 逐项恢复准备清单（2026-09-08）

本清单只整理两份既有 Mini receipt；没有新增全湖检查、canonical 读取或修复动作。精确候选 `38c90bb`；观测 benchmark `c11caa8`；Apex `ae57f93`。各 PR 尚未合并，未生产切换。

**269 个失败 symbol + 53 个窗口首日缩短的元数据差异，全部仍待逐项核验。清单完成不等于数据恢复完成或切换就绪。**

## 证据与边界

- `/Volumes/DATA_LAKE/livewire/disposable/se08-20260908T1122-c11caa8/logs/full-migration-failures.json`
  - SHA256 `7e5b2c4760307f7a10dd826bb698e57b0c771e7fed73a5d58424304f9d9f26ba`；1991070 bytes。
- `/Volumes/DATA_LAKE/livewire/disposable/se08-20260908T1122-c11caa8/logs/window-manifest-comparison.json`
  - SHA256 `b9b5af2ecb59c775800092258da094ce5959834030d04ff549c691876434bc39`；6644 bytes。

失败receipt生成于 `2026-09-08T03:39:04.360705+00:00`，as-of `2026-09-08`；其 `bronze_path` 指向内部临时复制输入 `/private/tmp/livewire-se08-20260908T1122-c11caa8-inputs/data-lake`，不是当前canonical位置，`source_sha256` 也仅标识该次复制输入。

窗口比较观测于 `2026-09-08T04:16:49.437851+00:00`，scope原文：`manifest metadata comparison; no correctness verification`。旧/新manifest各有13279成员：
- canonical_manifest: revision=39；`/Users/moremeds/market-warehouse/data-lake/silver/revisions/current.json`；SHA256 `06a2c50475e6dce6855df715969b0efacfec1331a6931ad7c36ae0f8f5d7c242`。
- fixture_manifest: revision=1；`/Volumes/DATA_LAKE/livewire/disposable/se08-20260908T1122-c11caa8/silver/revisions/current.json`；SHA256 `572d57ce916d97ecc67020645923352b0ef3b47426f71736eb437e11f2ef2bc1`。

53项是旧revision39与fixture revision1的`earliest_date`差异，不是此次failure-output的`window_regressions`（该字段为空），也不是已证实的算法回归。另有 **MUNJ** 首日由2026-08-27延长至2026-08-26，仅备注，不计入53；additions=0、withdrawals=0。

所有条目的“最后有效数据”均为**未知**。失败输入的覆盖日期、旧manifest记录以及fixture成功发布，都不能替代对应文件/语义/reader验证。RJF canonical/footer损坏依据[已有验证记录](2026-09-08-systematic-integrity-verification.md)：canonical与fixture字节的SHA256均为 `54e59febc7d33de3d0b91d557821dadc6c8116332ea6f1d54ebb2d37986c4051`；本次未复查，且没有替换授权。198/61/5/4各类也不得自动repair。

## 分类核验、准备动作与解除条件

以下动作均为待办，尚未执行；每个附录条目继承对应分类动作。原始全部字段（含active_actions）由上列receipt路径、SHA256和逐行JSON指针定位。任何恢复写入需另行明确批准。

### U — 价格基准未知（198）

- 只读核验：核对该symbol原始错误中的首个受split影响日期、原始行source/price_basis与active_actions身份；比对对应provider证据及split前后OHLCV，不能将unknown直接改为raw。
- 恢复准备：仅准备同一时间窗、同一action快照下的基准重建和差异清单，保存原始SHA与待审批候选；需要额外provider数据时另行安排只读获取。
- 解除条件：有可追溯证据确定价格基准；隔离候选通过split前后与连续性核验；任何canonical变更另获批准并通过正常发布，随后真实manifest选中且hash/reader验证通过。

### C — 股息币种不匹配（61）

- 只读核验：定位该symbol现金股息active_actions及其provider原始币种，核对证券/上市身份与Bronze报价币种；receipt未给出具体冲突币种，不能猜测。
- 恢复准备：整理股息金额、币种、除息日及证券身份对照，确定是否输入元数据问题或确需获批转换；不自动换汇或修改币种。
- 解除条件：币种与证券身份由原始证据解释清楚；获批候选股息调整及连续性通过，正常发布后的manifest和reader核验成功。

### S — 有效拆股冲突（5）

- 只读核验：按原始error给出的冲突日期、两个price factor和action_id，核对active_actions及provider原始事件、revision/supersession和证券身份；不按比例大小选真值。
- 恢复准备：准备事件级冲突对照与拟保留/撤销事件的证据、影响日期窗和候选输出差异；不自动取消事件。
- 解除条件：冲突事件有可追溯裁决，获批输入变更后有效split唯一且候选调整/连续性通过，发布与reader核验成功。

### D — 现金股息约束失败（4）

- 只读核验：从该symbol现有action身份定位触发约束的现金股息，核对除息日、金额/单位、币种、前一个有效close及其价格基准；receipt未给出具体日期/金额则保持未知。
- 恢复准备：准备股息与前收盘的原始证据和候选处理差异，区分特殊分派/单位/基准/事件问题，不擅自删除股息或压低金额。
- 解除条件：现金股息与前收盘约束有证据成立或有获批明确处理；候选通过调整与连续性核验，正常发布后hash和reader通过。

### R — RJF Parquet footer不可读（1）

- 只读核验：RJF canonical/footer损坏已由父任务确认；本清单仅复用该结论与复制输入receipt，未再次读取canonical。后续只核对已保留字节、footer诊断及备份/provider来源身份。
- 恢复准备：准备可验证替代候选的来源、时间覆盖、SHA、完整解码/OHLCV校验与回退材料；保留损坏原件，不执行替换。
- 解除条件：替代来源及候选验证完成且用户明确批准替换；正常canonical发布、Silver重建和manifest/reader验证成功。

### W — manifest首日缩短元数据差异（53）

- 只读核验：仅按两份已命名manifest的symbol/earliest_date定位差异；后续逐symbol比对旧新artifact hash、实际首日、输入as-of、action快照、seed/window裁剪及triage证据。元数据差异不能证明旧或新哪边正确。
- 恢复准备：准备旧/新首日、被排除日期段与裁剪原因的证据表；不得直接放宽窗口或恢复旧数据；如需修正先形成单symbol候选和审批材料。
- 解除条件：逐symbol有证据解释窗口变化并作出保留/修正裁决；如有修正，经批准后以新单调revision发布；实际覆盖、hash及reader验证与裁决一致。

## 逐项附录 A：269 个失败 symbol

每行证据指针相对于上列`full-migration-failures.json`原始receipt；分类动作见上文。原始SHA、完整输入路径与actions从对应`/failures/<index>`读取，不另建重复附录。

| ID | Symbol | 类别 | 原始错误 | 输入覆盖（非最后有效） | 最后有效 | 证据指针 |
|---|---|---|---|---|---|---|
| F001 | ACR | C | dividend currency does not match bronze currency | 2006-02-07 → 2026-09-04 | 未知 | `/failures/0` |
| F002 | AEYE | U | unknown price_basis for split-affected row 2014-02-13 | 2014-02-13 → 2026-09-04 | 未知 | `/failures/1` |
| F003 | AIG | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/2` |
| F004 | ALB | C | dividend currency does not match bronze currency | 1994-02-22 → 2026-09-04 | 未知 | `/failures/3` |
| F005 | ALC | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/4` |
| F006 | ALT | U | unknown price_basis for split-affected row 2017-05-05 | 2017-05-05 → 2026-09-04 | 未知 | `/failures/5` |
| F007 | AMC | U | unknown price_basis for split-affected row 2013-12-18 | 2013-12-18 → 2026-09-04 | 未知 | `/failures/6` |
| F008 | APLD | U | unknown price_basis for split-affected row 2009-12-10 | 2009-12-10 → 2026-09-04 | 未知 | `/failures/7` |
| F009 | ARKD | D | cash dividend must be less than positive previous close | 2023-11-15 → 2026-09-04 | 未知 | `/failures/8` |
| F010 | AROW | U | unknown price_basis for split-affected row 1980-09-18 | 1980-09-18 → 2026-09-04 | 未知 | `/failures/9` |
| F011 | ATLO | C | dividend currency does not match bronze currency | 1997-08-11 → 2026-09-04 | 未知 | `/failures/10` |
| F012 | ATRO | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/11` |
| F013 | AXON | U | unknown price_basis for split-affected row 2001-06-07 | 2001-06-07 → 2026-09-04 | 未知 | `/failures/12` |
| F014 | AXTU | U | unknown price_basis for split-affected row 2026-05-05 | 2026-05-05 → 2026-09-04 | 未知 | `/failures/13` |
| F015 | AZN | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/14` |
| F016 | BANL | U | unknown price_basis for split-affected row 2023-03-23 | 2023-03-23 → 2026-09-04 | 未知 | `/failures/15` |
| F017 | BBBY | U | unknown price_basis for split-affected row 2002-05-30 | 2002-05-30 → 2026-08-14 | 未知 | `/failures/16` |
| F018 | BBD | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/17` |
| F019 | BBDO | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/18` |
| F020 | BBLGW | U | unknown price_basis for split-affected row 2021-10-13 | 2021-10-13 → 2026-09-04 | 未知 | `/failures/19` |
| F021 | BCBP | U | unknown price_basis for split-affected row 2002-05-22 | 2002-05-22 → 2026-09-04 | 未知 | `/failures/20` |
| F022 | BCE | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/21` |
| F023 | BGSI | C | dividend currency does not match bronze currency | 2025-10-31 → 2026-09-04 | 未知 | `/failures/22` |
| F024 | BKEM | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/23` |
| F025 | BKIE | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/24` |
| F026 | BKLC | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/25` |
| F027 | BKMC | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/26` |
| F028 | BKSE | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/27` |
| F029 | BKTI | U | unknown price_basis for split-affected row 1980-03-18 | 1980-03-18 → 2026-09-04 | 未知 | `/failures/28` |
| F030 | BMO | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/29` |
| F031 | BNED | U | unknown price_basis for split-affected row 2015-07-23 | 2015-07-23 → 2026-09-04 | 未知 | `/failures/30` |
| F032 | BNS | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/31` |
| F033 | BRCC | U | unknown price_basis for split-affected row 2022-02-10 | 2022-02-10 → 2026-09-04 | 未知 | `/failures/32` |
| F034 | BTE | C | dividend currency does not match bronze currency | 2023-02-23 → 2026-09-04 | 未知 | `/failures/33` |
| F035 | BTX | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/34` |
| F036 | BVC | U | unknown price_basis for split-affected row 2025-12-24 | 2025-12-24 → 2026-09-04 | 未知 | `/failures/35` |
| F037 | BWLP | C | dividend currency does not match bronze currency | 2024-04-29 → 2026-09-04 | 未知 | `/failures/36` |
| F038 | BYND | U | unknown price_basis for split-affected row 2019-05-02 | 2019-05-02 → 2026-09-04 | 未知 | `/failures/37` |
| F039 | CALC | U | unknown price_basis for split-affected row 2023-03-21 | 2023-03-21 → 2026-09-04 | 未知 | `/failures/38` |
| F040 | CANG | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/39` |
| F041 | CASS | U | unknown price_basis for split-affected row 1996-07-02 | 1996-07-02 → 2026-09-04 | 未知 | `/failures/40` |
| F042 | CBIO | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/41` |
| F043 | CBRL | C | dividend currency does not match bronze currency | 1981-11-05 → 2026-09-04 | 未知 | `/failures/42` |
| F044 | CBSH | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/43` |
| F045 | CCEP | C | dividend currency does not match bronze currency | 1986-11-24 → 2026-09-04 | 未知 | `/failures/44` |
| F046 | CCG | U | unknown price_basis for split-affected row 2023-09-18 | 2023-09-18 → 2026-09-04 | 未知 | `/failures/45` |
| F047 | CCJ | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/46` |
| F048 | CCUP | U | unknown price_basis for split-affected row 2025-08-11 | 2025-08-11 → 2026-09-04 | 未知 | `/failures/47` |
| F049 | CDT | U | unknown price_basis for split-affected row 2023-09-25 | 2023-09-25 → 2026-09-04 | 未知 | `/failures/48` |
| F050 | CENT | U | unknown price_basis for split-affected row 1992-07-15 | 1992-07-15 → 2026-09-04 | 未知 | `/failures/49` |
| F051 | CGAU | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/50` |
| F052 | CLGN | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/51` |
| F053 | CM | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/52` |
| F054 | CMCSA | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/53` |
| F055 | CMCT | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/54` |
| F056 | CNC | U | unknown price_basis for split-affected row 2001-12-13 | 2001-12-13 → 2026-09-04 | 未知 | `/failures/55` |
| F057 | CNI | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/56` |
| F058 | CNOB | U | unknown price_basis for split-affected row 1994-04-04 | 1994-04-04 → 2026-09-04 | 未知 | `/failures/57` |
| F059 | CNQ | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/58` |
| F060 | COSM | U | unknown price_basis for split-affected row 2022-02-28 | 2022-02-28 → 2026-09-04 | 未知 | `/failures/59` |
| F061 | CP | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/60` |
| F062 | CRESY | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/61` |
| F063 | CRIS | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/62` |
| F064 | CRK | U | unknown price_basis for split-affected row 1987-08-28 | 1987-08-28 → 2026-09-04 | 未知 | `/failures/63` |
| F065 | CRWU | U | unknown price_basis for split-affected row 2025-07-25 | 2025-07-25 → 2026-09-04 | 未知 | `/failures/64` |
| F066 | CSAI | U | unknown price_basis for split-affected row 2025-01-30 | 2025-01-30 → 2026-09-04 | 未知 | `/failures/65` |
| F067 | CURX | U | unknown price_basis for split-affected row 2025-08-26 | 2025-08-26 → 2026-09-04 | 未知 | `/failures/66` |
| F068 | CVE | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/67` |
| F069 | CXAI | U | unknown price_basis for split-affected row 2023-03-15 | 2023-03-15 → 2026-09-04 | 未知 | `/failures/68` |
| F070 | CYN | U | unknown price_basis for split-affected row 2021-10-20 | 2021-10-20 → 2026-09-04 | 未知 | `/failures/69` |
| F071 | CZFS | U | unknown price_basis for split-affected row 2018-07-25 | 2018-07-25 → 2026-09-04 | 未知 | `/failures/70` |
| F072 | DB | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/71` |
| F073 | DBRG | D | cash dividend must be less than positive previous close | 2017-01-10 → 2026-09-04 | 未知 | `/failures/72` |
| F074 | DBVT | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/73` |
| F075 | DCTH | U | unknown price_basis for split-affected row 2017-05-22 | 2017-05-22 → 2026-09-04 | 未知 | `/failures/74` |
| F076 | DD | U | unknown price_basis for split-affected row 2017-09-01 | 2017-09-01 → 2026-09-04 | 未知 | `/failures/75` |
| F077 | DEC | C | dividend currency does not match bronze currency | 2023-12-18 → 2026-09-04 | 未知 | `/failures/76` |
| F078 | DFDV | U | unknown price_basis for split-affected row 2025-05-05 | 2025-05-05 → 2026-09-04 | 未知 | `/failures/77` |
| F079 | DFNS | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/78` |
| F080 | DOO | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/79` |
| F081 | DSX | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/80` |
| F082 | DXF | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/81` |
| F083 | ECL | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/82` |
| F084 | EDBL | U | unknown price_basis for split-affected row 2022-05-05 | 2022-05-05 → 2026-09-04 | 未知 | `/failures/83` |
| F085 | EFXT | C | dividend currency does not match bronze currency | 2022-10-13 → 2026-09-04 | 未知 | `/failures/84` |
| F086 | ELA | D | cash dividend must be less than positive previous close | 2004-01-23 → 2026-09-04 | 未知 | `/failures/85` |
| F087 | ELOX | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/86` |
| F088 | EMA | C | dividend currency does not match bronze currency | 2025-05-28 → 2026-09-04 | 未知 | `/failures/87` |
| F089 | ENB | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/88` |
| F090 | FBP | U | unknown price_basis for split-affected row 1987-01-13 | 1987-01-13 → 2026-09-04 | 未知 | `/failures/89` |
| F091 | FEED | U | unknown price_basis for split-affected row 2025-12-12 | 2025-12-12 → 2026-09-04 | 未知 | `/failures/90` |
| F092 | FER | C | dividend currency does not match bronze currency | 2024-05-09 → 2026-09-04 | 未知 | `/failures/91` |
| F093 | FFAI | U | unknown price_basis for split-affected row 2020-08-31 | 2020-08-31 → 2026-09-04 | 未知 | `/failures/92` |
| F094 | FFIN | U | unknown price_basis for split-affected row 1993-11-01 | 1993-11-01 → 2026-09-04 | 未知 | `/failures/93` |
| F095 | FLNG | C | dividend currency does not match bronze currency | 2019-06-17 → 2026-09-04 | 未知 | `/failures/94` |
| F096 | FTLF | S | conflicting active splits on 2019-04-16: price factor 0.00125 vs 8000 (8ecc168cb6e63ea43756551699d8a0b2) | 2013-10-01 → 2026-09-04 | 未知 | `/failures/95` |
| F097 | FTS | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/96` |
| F098 | GIB | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/97` |
| F099 | GITS | U | unknown price_basis for split-affected row 2024-12-18 | 2024-12-18 → 2026-09-04 | 未知 | `/failures/98` |
| F100 | GNPX | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/99` |
| F101 | GOCO | U | unknown price_basis for split-affected row 2020-07-15 | 2020-07-15 → 2026-06-15 | 未知 | `/failures/100` |
| F102 | GOLD | U | unknown price_basis for split-affected row 2014-03-17 | 2014-03-17 → 2026-09-04 | 未知 | `/failures/101` |
| F103 | GOOD | C | dividend currency does not match bronze currency | 2003-08-13 → 2026-09-04 | 未知 | `/failures/102` |
| F104 | GRML | U | unknown price_basis for split-affected row 2026-03-12 | 2026-03-12 → 2026-09-04 | 未知 | `/failures/103` |
| F105 | GTY | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/104` |
| F106 | GYRE | U | unknown price_basis for split-affected row 2006-04-12 | 2006-04-12 → 2026-09-04 | 未知 | `/failures/105` |
| F107 | HAO | U | unknown price_basis for split-affected row 2024-01-26 | 2024-01-26 → 2026-09-04 | 未知 | `/failures/106` |
| F108 | HBM | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/107` |
| F109 | HCWC | U | unknown price_basis for split-affected row 2024-09-16 | 2024-09-16 → 2026-09-04 | 未知 | `/failures/108` |
| F110 | HON | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/109` |
| F111 | HOV | U | unknown price_basis for split-affected row 1983-09-07 | 1983-09-07 → 2026-09-04 | 未知 | `/failures/110` |
| F112 | HURA | U | unknown price_basis for split-affected row 2014-07-18 | 2014-07-18 → 2026-09-04 | 未知 | `/failures/111` |
| F113 | HWBK | U | unknown price_basis for split-affected row 2000-06-21 | 2000-06-21 → 2026-09-04 | 未知 | `/failures/112` |
| F114 | IBOC | U | unknown price_basis for split-affected row 1995-08-30 | 1995-08-30 → 2026-09-04 | 未知 | `/failures/113` |
| F115 | ICL | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/114` |
| F116 | ICON | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/115` |
| F117 | IDT | U | unknown price_basis for split-affected row 2001-06-01 | 2001-06-01 → 2026-09-04 | 未知 | `/failures/116` |
| F118 | IESC | U | unknown price_basis for split-affected row 2006-05-15 | 2006-05-15 → 2026-09-04 | 未知 | `/failures/117` |
| F119 | IMO | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/118` |
| F120 | IMRA | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-07-31 | 未知 | `/failures/119` |
| F121 | INCR | U | unknown price_basis for split-affected row 2021-09-01 | 2021-09-01 → 2026-09-04 | 未知 | `/failures/120` |
| F122 | IREZ | U | unknown price_basis for split-affected row 2026-01-22 | 2026-01-22 → 2026-09-04 | 未知 | `/failures/121` |
| F123 | ITUB | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/122` |
| F124 | IXHL | U | unknown price_basis for split-affected row 2022-03-02 | 2022-03-02 → 2026-09-04 | 未知 | `/failures/123` |
| F125 | JAGX | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/124` |
| F126 | JUNS | U | unknown price_basis for split-affected row 2024-12-03 | 2024-12-03 → 2026-09-04 | 未知 | `/failures/125` |
| F127 | JXG | U | unknown price_basis for split-affected row 2024-12-20 | 2024-12-20 → 2026-09-04 | 未知 | `/failures/126` |
| F128 | KAPA | U | unknown price_basis for split-affected row 2024-09-16 | 2024-09-16 → 2026-09-04 | 未知 | `/failures/127` |
| F129 | KORE | U | unknown price_basis for split-affected row 2021-10-01 | 2021-10-01 → 2026-07-20 | 未知 | `/failures/128` |
| F130 | KWM | U | unknown price_basis for split-affected row 2025-05-14 | 2025-05-14 → 2026-09-04 | 未知 | `/failures/129` |
| F131 | LADR | S | conflicting active splits on 2015-12-08: price factor 0.9110936586059173710939137121 vs 0.8874271976912694024863935225 (dbf3096a04e82eeb6843c6aed34a291d) | 2014-02-06 → 2026-09-04 | 未知 | `/failures/130` |
| F132 | LARK | U | unknown price_basis for split-affected row 1994-03-28 | 1994-03-28 → 2026-09-04 | 未知 | `/failures/131` |
| F133 | LEN | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/132` |
| F134 | LEXX | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/133` |
| F135 | LFDR | U | unknown price_basis for split-affected row 2024-12-16 | 2024-12-16 → 2026-09-04 | 未知 | `/failures/134` |
| F136 | LICN | U | unknown price_basis for split-affected row 2023-02-06 | 2023-02-06 → 2026-09-04 | 未知 | `/failures/135` |
| F137 | LILA | U | unknown price_basis for split-affected row 2015-06-22 | 2015-06-22 → 2026-09-04 | 未知 | `/failures/136` |
| F138 | LILAK | U | unknown price_basis for split-affected row 2015-06-23 | 2015-06-23 → 2026-09-04 | 未知 | `/failures/137` |
| F139 | LIMN | U | unknown price_basis for split-affected row 2025-05-01 | 2025-05-01 → 2026-09-04 | 未知 | `/failures/138` |
| F140 | LOGI | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/139` |
| F141 | MBAI | U | unknown price_basis for split-affected row 2025-11-28 | 2025-11-28 → 2026-09-04 | 未知 | `/failures/140` |
| F142 | MCBS | U | unknown price_basis for split-affected row 2017-01-19 | 2017-01-19 → 2026-09-04 | 未知 | `/failures/141` |
| F143 | MCHB | D | cash dividend must be less than positive previous close | 2012-02-13 → 2026-09-04 | 未知 | `/failures/142` |
| F144 | MDRR | S | conflicting active splits on 2024-07-03: price factor 10 vs 0.2 (e24ba650ab17cb1f7123bdd8c6351c66) | 2021-06-11 → 2026-09-04 | 未知 | `/failures/143` |
| F145 | METC | U | unknown price_basis for split-affected row 2017-02-03 | 2017-02-03 → 2026-09-04 | 未知 | `/failures/144` |
| F146 | MFC | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/145` |
| F147 | MI | U | unknown price_basis for split-affected row 2023-09-18 | 2023-09-18 → 2026-09-04 | 未知 | `/failures/146` |
| F148 | MMM | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/147` |
| F149 | MMS | U | unknown price_basis for split-affected row 1997-06-13 | 1997-06-13 → 2026-09-04 | 未知 | `/failures/148` |
| F150 | MRDN | U | unknown price_basis for split-affected row 2012-07-23 | 2012-07-23 → 2026-09-04 | 未知 | `/failures/149` |
| F151 | MSGY | U | unknown price_basis for split-affected row 2025-07-08 | 2025-07-08 → 2026-09-04 | 未知 | `/failures/150` |
| F152 | MSI | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/151` |
| F153 | MSTR | U | unknown price_basis for split-affected row 1998-06-11 | 1998-06-11 → 2026-09-04 | 未知 | `/failures/152` |
| F154 | MTA | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/153` |
| F155 | MUR | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/154` |
| F156 | NEOG | U | unknown price_basis for split-affected row 1990-05-29 | 1990-05-29 → 2026-09-04 | 未知 | `/failures/155` |
| F157 | NEXR | U | unknown price_basis for split-affected row 2026-03-31 | 2026-03-31 → 2026-09-04 | 未知 | `/failures/156` |
| F158 | NGG | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/157` |
| F159 | NIVF | U | unknown price_basis for split-affected row 2024-04-04 | 2024-04-04 → 2026-09-04 | 未知 | `/failures/158` |
| F160 | NJR | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/159` |
| F161 | NOA | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/160` |
| F162 | NPKI | C | dividend currency does not match bronze currency | 1990-09-06 → 2026-09-04 | 未知 | `/failures/161` |
| F163 | NRDY | U | unknown price_basis for split-affected row 2020-11-27 | 2020-11-27 → 2026-09-04 | 未知 | `/failures/162` |
| F164 | NTB | C | dividend currency does not match bronze currency | 2016-09-16 → 2026-09-04 | 未知 | `/failures/163` |
| F165 | NVRI | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/164` |
| F166 | NVX | U | unknown price_basis for split-affected row 2022-02-02 | 2022-02-02 → 2026-09-04 | 未知 | `/failures/165` |
| F167 | NXL | U | unknown price_basis for split-affected row 2022-09-16 | 2022-09-16 → 2026-09-04 | 未知 | `/failures/166` |
| F168 | OCCI | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/167` |
| F169 | OFAL | U | unknown price_basis for split-affected row 2025-05-21 | 2025-05-21 → 2026-09-04 | 未知 | `/failures/168` |
| F170 | ONFO | U | unknown price_basis for split-affected row 2022-08-26 | 2022-08-26 → 2026-09-04 | 未知 | `/failures/169` |
| F171 | OR | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/170` |
| F172 | ORGN | U | unknown price_basis for split-affected row 2021-06-25 | 2021-06-25 → 2026-07-01 | 未知 | `/failures/171` |
| F173 | ORIS | U | unknown price_basis for split-affected row 2024-10-17 | 2024-10-17 → 2026-06-23 | 未知 | `/failures/172` |
| F174 | OUT | S | conflicting active splits on 2014-11-18: price factor 0.8915834522111269614835948645 vs 0.8530967411704487288858556560 (69a3b1ca1bfe37bce59c82955af6e31f) | 2014-03-28 → 2026-09-04 | 未知 | `/failures/173` |
| F175 | PATK | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/174` |
| F176 | PBA | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/175` |
| F177 | PDEX | U | unknown price_basis for split-affected row 1988-01-04 | 1988-01-04 → 2026-09-04 | 未知 | `/failures/176` |
| F178 | PEBK | U | unknown price_basis for split-affected row 1985-10-16 | 1985-10-16 → 2026-09-04 | 未知 | `/failures/177` |
| F179 | PII | U | unknown price_basis for split-affected row 1987-09-16 | 1987-09-16 → 2026-09-04 | 未知 | `/failures/178` |
| F180 | PKBK | U | unknown price_basis for split-affected row 2002-07-09 | 2002-07-09 → 2026-09-04 | 未知 | `/failures/179` |
| F181 | PLUS | U | unknown price_basis for split-affected row 1996-11-15 | 1996-11-15 → 2026-09-04 | 未知 | `/failures/180` |
| F182 | PMI | U | unknown price_basis for split-affected row 2025-08-29 | 2025-08-29 → 2026-09-04 | 未知 | `/failures/181` |
| F183 | PONX | U | unknown price_basis for split-affected row 2025-09-09 | 2025-09-09 → 2026-09-04 | 未知 | `/failures/182` |
| F184 | PPTA | U | unknown price_basis for split-affected row 2011-10-07 | 2011-10-07 → 2026-09-04 | 未知 | `/failures/183` |
| F185 | PRFX | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/184` |
| F186 | PRGS | U | unknown price_basis for split-affected row 1991-07-30 | 1991-07-30 → 2026-09-04 | 未知 | `/failures/185` |
| F187 | PROP | U | unknown price_basis for split-affected row 2011-03-02 | 2011-03-02 → 2026-09-04 | 未知 | `/failures/186` |
| F188 | PRPL | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/187` |
| F189 | PSQH | U | unknown price_basis for split-affected row 2023-07-20 | 2023-07-20 → 2026-09-04 | 未知 | `/failures/188` |
| F190 | QGEN | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/189` |
| F191 | RACE | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/190` |
| F192 | RCI | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/191` |
| F193 | RCL | C | dividend currency does not match bronze currency | 1993-04-28 → 2026-09-04 | 未知 | `/failures/192` |
| F194 | REE | U | unknown price_basis for split-affected row 2021-07-23 | 2021-07-23 → 2026-07-06 | 未知 | `/failures/193` |
| F195 | REPX | U | unknown price_basis for split-affected row 1994-08-26 | 1994-08-26 → 2026-09-04 | 未知 | `/failures/194` |
| F196 | RETO | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/195` |
| F197 | RIGL | U | unknown price_basis for split-affected row 2000-11-29 | 2000-11-29 → 2026-09-04 | 未知 | `/failures/196` |
| F198 | RJF | R | Parquet magic bytes not found in footer. Either the file is corrupted or this is not a parquet file. | 未知 → 未知 | 未知 | `/failures/197` |
| F199 | RNAC | U | unknown price_basis for split-affected row 2016-06-22 | 2016-06-22 → 2026-09-04 | 未知 | `/failures/198` |
| F200 | ROL | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/199` |
| F201 | RVSN | U | unknown price_basis for split-affected row 2022-03-31 | 2022-03-31 → 2026-09-04 | 未知 | `/failures/200` |
| F202 | RY | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/201` |
| F203 | RYN | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/202` |
| F204 | RZLT | U | unknown price_basis for split-affected row 2013-06-11 | 2013-06-11 → 2026-09-04 | 未知 | `/failures/203` |
| F205 | SANM | U | unknown price_basis for split-affected row 1993-04-14 | 1993-04-14 → 2026-09-04 | 未知 | `/failures/204` |
| F206 | SBSI | U | unknown price_basis for split-affected row 1996-10-30 | 1996-10-30 → 2026-09-04 | 未知 | `/failures/205` |
| F207 | SBTU | U | unknown price_basis for split-affected row 2025-10-21 | 2025-10-21 → 2026-09-04 | 未知 | `/failures/206` |
| F208 | SKYE | U | unknown price_basis for split-affected row 2024-04-11 | 2024-04-11 → 2026-09-04 | 未知 | `/failures/207` |
| F209 | SLF | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/208` |
| F210 | SLG | S | conflicting active splits on 2020-12-14: price factor 0.9668374746205162912114473557 vs 0.9728572818367545481077925868 (e9e1c8a6be92ecfc91d3ec93867d356a) | 1997-08-15 → 2026-09-04 | 未知 | `/failures/209` |
| F211 | SMST | U | unknown price_basis for split-affected row 2024-08-21 | 2024-08-21 → 2026-09-04 | 未知 | `/failures/210` |
| F212 | SMTK | U | unknown price_basis for split-affected row 2024-05-31 | 2024-05-31 → 2026-09-04 | 未知 | `/failures/211` |
| F213 | SMX | U | unknown price_basis for split-affected row 2023-03-08 | 2023-03-08 → 2026-09-04 | 未知 | `/failures/212` |
| F214 | SMXT | U | unknown price_basis for split-affected row 2024-02-27 | 2024-02-27 → 2026-09-04 | 未知 | `/failures/213` |
| F215 | SNDA | U | unknown price_basis for split-affected row 1997-10-31 | 1997-10-31 → 2026-09-04 | 未知 | `/failures/214` |
| F216 | SNDQ | U | unknown price_basis for split-affected row 2026-04-23 | 2026-04-23 → 2026-09-04 | 未知 | `/failures/215` |
| F217 | SNFCA | U | unknown price_basis for split-affected row 1987-08-12 | 1987-08-12 → 2026-09-04 | 未知 | `/failures/216` |
| F218 | SNWV | U | unknown price_basis for split-affected row 2015-03-11 | 2015-03-11 → 2026-09-04 | 未知 | `/failures/217` |
| F219 | SOXS | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/218` |
| F220 | SPRB | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/219` |
| F221 | SRCE | U | unknown price_basis for split-affected row 1983-08-12 | 1983-08-12 → 2026-09-04 | 未知 | `/failures/220` |
| F222 | SRL | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/221` |
| F223 | SSP | U | unknown price_basis for split-affected row 1988-06-30 | 1988-06-30 → 2026-09-04 | 未知 | `/failures/222` |
| F224 | STLA | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/223` |
| F225 | STN | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/224` |
| F226 | STRC | U | unknown price_basis for split-affected row 2021-09-27 | 2021-09-27 → 2026-09-04 | 未知 | `/failures/225` |
| F227 | STVN | C | dividend currency does not match bronze currency | 2021-07-16 → 2026-09-04 | 未知 | `/failures/226` |
| F228 | STXS | U | unknown price_basis for split-affected row 2004-08-12 | 2004-08-12 → 2026-09-04 | 未知 | `/failures/227` |
| F229 | SU | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/228` |
| F230 | SVRE | U | unknown price_basis for split-affected row 2022-06-03 | 2022-06-03 → 2026-09-04 | 未知 | `/failures/229` |
| F231 | SXTC | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/230` |
| F232 | TAC | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/231` |
| F233 | TD | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/232` |
| F234 | TECK | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/233` |
| F235 | TEUP | U | unknown price_basis for split-affected row 2026-05-29 | 2026-05-29 → 2026-09-04 | 未知 | `/failures/234` |
| F236 | THH | U | unknown price_basis for split-affected row 2025-08-28 | 2025-08-28 → 2026-09-04 | 未知 | `/failures/235` |
| F237 | TMP | U | unknown price_basis for split-affected row 1986-06-02 | 1986-06-02 → 2026-09-04 | 未知 | `/failures/236` |
| F238 | TOMZ | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-04 | 未知 | `/failures/237` |
| F239 | TOP | U | unknown price_basis for split-affected row 2022-06-01 | 2022-06-01 → 2026-09-04 | 未知 | `/failures/238` |
| F240 | TR | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/239` |
| F241 | TRC | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/240` |
| F242 | TRI | U | unknown price_basis for split-affected row 2002-06-12 | 2002-06-12 → 2026-09-04 | 未知 | `/failures/241` |
| F243 | TRP | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/242` |
| F244 | TTDU | U | unknown price_basis for split-affected row 2025-09-17 | 2025-09-17 → 2026-09-04 | 未知 | `/failures/243` |
| F245 | TTE | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/244` |
| F246 | TU | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/245` |
| F247 | UNTY | U | unknown price_basis for split-affected row 1998-09-21 | 1998-09-21 → 2026-09-04 | 未知 | `/failures/246` |
| F248 | VABK | U | unknown price_basis for split-affected row 2012-07-26 | 2012-07-26 → 2026-09-04 | 未知 | `/failures/247` |
| F249 | VBIO | U | unknown price_basis for split-affected row 2026-04-28 | 2026-04-28 → 2026-09-04 | 未知 | `/failures/248` |
| F250 | VBNK | C | dividend currency does not match bronze currency | 2021-09-22 → 2026-09-04 | 未知 | `/failures/249` |
| F251 | VEEA | U | unknown price_basis for split-affected row 2024-09-17 | 2024-09-17 → 2026-09-04 | 未知 | `/failures/250` |
| F252 | VET | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/251` |
| F253 | VIA | U | unknown price_basis for split-affected row 2021-08-10 | 2021-08-10 → 2026-09-04 | 未知 | `/failures/252` |
| F254 | VIVK | U | unknown price_basis for split-affected row 2022-02-14 | 2022-02-14 → 2026-09-04 | 未知 | `/failures/253` |
| F255 | VLGEA | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/254` |
| F256 | VLO | U | unknown price_basis for split-affected row 1997-08-01 | 1997-08-01 → 2026-09-04 | 未知 | `/failures/255` |
| F257 | WETO | U | unknown price_basis for split-affected row 2025-02-27 | 2025-02-27 → 2026-09-04 | 未知 | `/failures/256` |
| F258 | WFG | C | dividend currency does not match bronze currency | 2021-06-11 → 2026-09-04 | 未知 | `/failures/257` |
| F259 | WKC | U | unknown price_basis for split-affected row 1986-08-28 | 1986-08-28 → 2026-09-04 | 未知 | `/failures/258` |
| F260 | WLDSW | U | unknown price_basis for split-affected row 2022-09-13 | 2022-09-13 → 2026-09-04 | 未知 | `/failures/259` |
| F261 | WLFC | U | unknown price_basis for split-affected row 1996-09-18 | 1996-09-18 → 2026-09-04 | 未知 | `/failures/260` |
| F262 | WST | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/261` |
| F263 | WTO | U | unknown price_basis for split-affected row 2023-09-05 | 2023-09-05 → 2026-07-01 | 未知 | `/failures/262` |
| F264 | WY | U | unknown price_basis for split-affected row 1980-03-17 | 1980-03-17 → 2026-09-04 | 未知 | `/failures/263` |
| F265 | WZRD | U | unknown price_basis for split-affected row 2024-03-20 | 2024-03-20 → 2026-09-04 | 未知 | `/failures/264` |
| F266 | XCH | U | unknown price_basis for split-affected row 2024-09-10 | 2024-09-10 → 2026-09-04 | 未知 | `/failures/265` |
| F267 | YHC | U | unknown price_basis for split-affected row 2024-12-16 | 2024-12-16 → 2026-09-04 | 未知 | `/failures/266` |
| F268 | ZGN | C | dividend currency does not match bronze currency | 2021-01-11 → 2026-09-04 | 未知 | `/failures/267` |
| F269 | ZKIN | U | unknown price_basis for split-affected row 2021-06-11 | 2021-06-11 → 2026-09-03 | 未知 | `/failures/268` |

## 逐项附录 B：53 个窗口首日缩短差异

每行仅陈述元数据。证据指针相对于`window-manifest-comparison.json`；全部使用W核验/准备/解除条件，不预判旧或新正确。

| ID | Symbol | 旧首日 | 新首日 | 类别 | 最后有效 | 证据指针 |
|---|---|---|---|---|---|---|
| W001 | AACBR | 2025-04-07 | 2026-08-14 | W | 未知 | `/shortened/0` |
| W002 | ABEO | 2012-12-28 | 2015-05-22 | W | 未知 | `/shortened/1` |
| W003 | AIM | 2021-06-11 | 2025-06-17 | W | 未知 | `/shortened/2` |
| W004 | ALPS | 2022-09-28 | 2025-10-31 | W | 未知 | `/shortened/3` |
| W005 | AMCR | 2019-06-11 | 2021-06-11 | W | 未知 | `/shortened/4` |
| W006 | ANSCW | 2024-01-03 | 2026-08-03 | W | 未知 | `/shortened/5` |
| W007 | APUS | 2025-05-09 | 2026-03-26 | W | 未知 | `/shortened/6` |
| W008 | BNY | 2021-06-11 | 2026-05-21 | W | 未知 | `/shortened/7` |
| W009 | BNZI | 2023-12-15 | 2026-05-08 | W | 未知 | `/shortened/8` |
| W010 | CCIXW | 2024-06-21 | 2026-07-15 | W | 未知 | `/shortened/9` |
| W011 | CELUW | 2021-07-19 | 2026-07-17 | W | 未知 | `/shortened/10` |
| W012 | CPHI | 2021-06-11 | 2026-07-21 | W | 未知 | `/shortened/11` |
| W013 | CSX | 2015-12-22 | 2021-06-11 | W | 未知 | `/shortened/12` |
| W014 | CTO | 2021-02-03 | 2021-06-11 | W | 未知 | `/shortened/13` |
| W015 | DELL | 2018-12-21 | 2021-06-11 | W | 未知 | `/shortened/14` |
| W016 | DKI | 2025-08-08 | 2025-09-30 | W | 未知 | `/shortened/15` |
| W017 | FCUV | 2021-08-31 | 2026-07-31 | W | 未知 | `/shortened/16` |
| W018 | FGIWW | 2022-01-25 | 2026-07-28 | W | 未知 | `/shortened/17` |
| W019 | GDEVW | 2023-03-20 | 2026-08-25 | W | 未知 | `/shortened/18` |
| W020 | GFAIW | 2021-09-29 | 2026-08-27 | W | 未知 | `/shortened/19` |
| W021 | HHH | 2021-10-20 | 2023-08-14 | W | 未知 | `/shortened/20` |
| W022 | LBGJ | 2026-02-27 | 2026-07-20 | W | 未知 | `/shortened/21` |
| W023 | MZTI | 1980-03-17 | 2010-05-24 | W | 未知 | `/shortened/22` |
| W024 | NCL | 2023-10-19 | 2023-12-19 | W | 未知 | `/shortened/23` |
| W025 | NCT | 2025-03-28 | 2026-09-03 | W | 未知 | `/shortened/24` |
| W026 | OII | 1980-03-17 | 2026-09-04 | W | 未知 | `/shortened/25` |
| W027 | OPI | 2021-06-11 | 2026-06-22 | W | 未知 | `/shortened/26` |
| W028 | PAMT | 1994-05-27 | 2005-10-18 | W | 未知 | `/shortened/27` |
| W029 | PFSA | 2025-07-14 | 2026-08-18 | W | 未知 | `/shortened/28` |
| W030 | PLAG | 2021-06-11 | 2026-08-11 | W | 未知 | `/shortened/29` |
| W031 | POM | 2025-10-08 | 2026-05-08 | W | 未知 | `/shortened/30` |
| W032 | PSTV | 2021-06-11 | 2026-04-02 | W | 未知 | `/shortened/31` |
| W033 | RCKTW | 2023-02-27 | 2026-09-02 | W | 未知 | `/shortened/32` |
| W034 | RCON | 2021-06-11 | 2026-08-05 | W | 未知 | `/shortened/33` |
| W035 | RITR | 2024-08-23 | 2026-08-03 | W | 未知 | `/shortened/34` |
| W036 | RNWWW | 2021-08-24 | 2026-08-17 | W | 未知 | `/shortened/35` |
| W037 | RVI | 2021-06-11 | 2026-03-06 | W | 未知 | `/shortened/36` |
| W038 | SION | 2025-02-07 | 2026-08-10 | W | 未知 | `/shortened/37` |
| W039 | SKK | 2024-10-08 | 2026-05-04 | W | 未知 | `/shortened/38` |
| W040 | SMH | 2011-12-21 | 2021-06-11 | W | 未知 | `/shortened/39` |
| W041 | SMJF | 2025-12-04 | 2026-08-27 | W | 未知 | `/shortened/40` |
| W042 | SRE | 2006-05-24 | 2021-06-11 | W | 未知 | `/shortened/41` |
| W043 | STAK | 2025-02-26 | 2026-07-24 | W | 未知 | `/shortened/42` |
| W044 | TENX | 2021-06-11 | 2026-08-10 | W | 未知 | `/shortened/43` |
| W045 | VIVO | 2021-06-11 | 2026-03-16 | W | 未知 | `/shortened/44` |
| W046 | VSEEW | 2024-06-25 | 2026-08-12 | W | 未知 | `/shortened/45` |
| W047 | VSTD | 2025-09-03 | 2026-08-24 | W | 未知 | `/shortened/46` |
| W048 | WRB | 2006-11-16 | 2021-06-11 | W | 未知 | `/shortened/47` |
| W049 | WW | 2021-06-11 | 2025-07-07 | W | 未知 | `/shortened/48` |
| W050 | XLE | 2021-05-18 | 2021-06-11 | W | 未知 | `/shortened/49` |
| W051 | XOSWW | 2026-06-03 | 2026-08-18 | W | 未知 | `/shortened/50` |
| W052 | YXT | 2024-08-16 | 2026-08-05 | W | 未知 | `/shortened/51` |
| W053 | ZYBT | 2025-01-07 | 2026-07-20 | W | 未知 | `/shortened/52` |

## 数量及身份自检

- 失败：198 + 61 + 5 + 4 + 1 = **269**，269个唯一symbol，无漏项/重复。
- shortened：**53**行、53个唯一symbol；与失败symbol集合交集为0。
- 长窗口MUNJ单独备注，不混入53；表格symbol、原始错误、输入覆盖和窗口日期均已逐项核对原始receipt。
- 共322个待核验条目，最后有效数据均未知；未赋予自动修复授权。
- 证据以两份原始receipt为准；本清单通过其完整路径、SHA256与逐行JSON指针追溯，不依赖额外复制的JSON文件。
