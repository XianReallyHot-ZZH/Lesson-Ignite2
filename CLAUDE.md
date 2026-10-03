# Lesson-Ignite2

以课程（lesson）形式从零复刻 Apache Ignite 2.18.0。总路线、课程骨架与进度追踪见 [ROADMAP.md](ROADMAP.md)。`vendors/ignite/` 是官方源码 submodule（primary source，只读参照），不是本项目要构建的代码。

## Agent skills

### Issue tracker

Issues 跟踪在本仓库的 GitHub Issues，全部操作走 `gh` CLI。See `docs/agents/issue-tracker.md`.

### Triage labels

采用默认五个 triage 标签：`needs-triage` / `needs-info` / `ready-for-agent` / `ready-for-human` / `wontfix`。See `docs/agents/triage-labels.md`.

### Domain docs

Single-context 布局：根 `CONTEXT.md` + `docs/adr/`（由 `/grill-with-docs`、`/domain-modeling` 懒创建）。See `docs/agents/domain.md`.
