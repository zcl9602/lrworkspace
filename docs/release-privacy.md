# 0.5.1 隐私修复与发布检查

本次修复两个已复现的问题：编译器将构建账户、依赖和输出目录嵌入程序；旧 `Key.passphrase` 派生序列化后会保存到注册文件。源码搜索不能代替二进制检查。

## 注册与旧字段

私钥口令使用共享、可清零的秘密容器，Debug/Serialize 不提供明文；`key.passphrase` 不参与持久化序列化。加载旧字段时仅在本次进程中保留，供旧库接口兼容，过滤模式保留至进程退出；下一次保存注册即省略。之后使用系统 SSH agent 或已解锁密钥，不能依靠 JSON 保存私钥口令。

端口修改的备份改为通过安全序列化生成，保留原主机、端口、目录等元数据，省略口令，包括 jump host 的口令。旧版 `password` 认证对象不可再次保存或复制为新备份；先通过 `workspace_login` 或 SSH agent 替换认证方式。移除了库后端读取 `LRWORKSPACE_SSH_PASSWORD` 的隐式回退。桌面本机窗口的密码会话保持现有进程边界。

已有旧注册在重新保存前仍可能含旧口令；已有私人备份不会自动改写或删除。升级不会宣称清除了所有旧副本，也不删除用户数据。需要处理旧文件时，应先盘点具体文件，仅清理凭据字段，保留工作区与作业记录。当前本机审计没有检出非空旧口令；合成回归覆盖旧记录加载、保存、新备份、失败保留和显式认证迁移。

## 发布构建

构建发布程序使用 Python 标准库包装器，最终用户运行仍无需 Python：

```text
python scripts/build_release.py --verify-install
python scripts/package_release.py --bin-dir target/release --out-dir dist --platform windows --arch x86_64 --source-commit <完整40位Git提交>
```

替换目标平台和架构；`--target-dir` 可指定独立构建目录。`--verify-password` 额外运行 localhost 合成认证测试，需测试依赖 Paramiko。包装器保留已有 Rust 构建参数，使用编码参数列表处理中文和空格路径，并对当前源码、用户、Cargo、Rustup、输出和临时目录设置路径映射。Rust 与原生 C/C++ 同时处理，发布配置剥离符号；不对已编译程序做字符串替换。

路径映射依据 [Rust 编译器文档](https://doc.rust-lang.org/rustc/remap-source-paths.html)，符号剥离依据 [Cargo profile 文档](https://doc.rust-lang.org/cargo/reference/profiles.html#strip)。这些设置不等于隐私证明，因此构建后和打包前都必须扫描三个可执行程序：

- 当前用户、源码与输出目录的 UTF-8、UTF-16 和常见路径写法，以及 COFF 符号中的目录变体。
- 未映射的 Windows/macOS/Linux 用户目录，以及常见 Token 和私钥材料模式。
- 文档中的当前私有根和凭据材料；文档中明确标注的虚构用户路径仍可保留。

Unix 的 vendored OpenSSL 会把 CFLAGS 放进版本信息，因此本机路径映射通过临时编译器包装器注入，不写入可被其记录的 CFLAGS。包装器只在构建时使用；静态构建生成的临时 engine/module 默认目录改为系统标准目录，运行时仍可用 OpenSSL 自身的配置覆盖。包装器版本参与缓存键，避免旧原生对象被错误复用。已知 OpenSSH 安全密钥算法名与 API Token 的共同前缀单独区分；UTF-16 的常见路径/凭据也检查。

失败时只报告文件名和问题类别，不回显匹配值；发现问题就拒绝打包。剥离符号本身不能删除 `.rdata` 中的诊断路径；必须重新编译并检查实际程序。手动绕过包装器编译的程序仍受打包检查约束。

测试使用合成路径和假口令，不验证任何真实凭据。扫描是防回归措施，不能证明不存在任意未知编码的秘密或同权限内存访问。四平台发布工作流在托管 runner 构建、验证并打包；具体结果以对应提交的运行记录为准。
