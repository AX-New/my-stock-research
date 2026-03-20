---
description: 刷新项目文档并提交推送，用于会话结束前同步上下文
---

# 同步文档 & 提交推送
**注意：同时修改了多个项目，要针对不同的项目目录进行处理，不能只在一个项目里面更新**
1. `git diff --name-only origin/master` + `git status` 盘点改动
2. 按需刷新文档（先读再改，只改有变化的部分）：
   - `task/project/` — 更新进度打勾
   - `PROGRESS.md` — 新踩坑记录
   - `docs/arc/` — 新增/变更的架构
   - `README.md` — 新功能一句话
3. `git pull origin master` → `git add` → `git commit` → `git push origin master`
4. 汇报：更新了什么、commit 列表、push 结果
