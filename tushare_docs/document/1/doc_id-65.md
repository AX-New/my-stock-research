## pip安装超时的解决方案

作者： Tushare社区用户 晓子

在中国大陆使用pip进行python包安装的时候经常会出现socket.timeout: The read operation timed out的问题，下面就讲讲解决方案。

### >> 解决方案 <<

使用国内镜像（以安装tushare pro为例）

```
pip install tushare -i https://pypi.tuna.tsinghua.edu.cn/simple/
```

### >> 深入探讨 <<

下面仔细说说上述问题并深入探讨下国内镜像的配置。

出现超时，主要是因为PyPI（pip命令的包）使用的源在国外，导致大陆链接速度过慢，进而引起超时。故而，我们可以使用国内的镜像来下载安装包。下面列举国内常用的一些安装镜像：

| 镜像 | 链接 |
| --- | --- |
| 阿里云 | http://mirrors.aliyun.com/pypi/simple/ |
| 中国科技大学 | https://pypi.mirrors.ustc.edu.cn/simple/ |
| 豆瓣(douban) | http://pypi.douban.com/simple/ |
| 清华大学 | https://pypi.tuna.tsinghua.edu.cn/simple/ |
| 中国科学技术大学 | http://pypi.mirrors.ustc.edu.cn/simple/ |

#### 镜像的使用方法

在使用pip时传递-i及相应的镜像地址即可（见以下tushare pro的安装）

```
pip install tushare -i https://pypi.tuna.tsinghua.edu.cn/simple/
```

#### not a trusted or secure host 问题

如果在使用某个镜像时遇到如下的 not a trusted or secure host 提醒，并且确认该host是可信赖的，可以按照提示添加 --trusted-host 及该host链接来进行安装。

```
The repository located at pypi.douban.com is not a trusted or secure host and is being ignored. If this repository is available via HTTPS we recommend you use HTTPS instead, otherw
ise you may silence this warning and allow it anyway with '--trusted-host pypi.douban.com'.
```

#### 配置默认镜像

如果觉得每次安装时添加镜像链接比较麻烦，可以将该镜像链接配置成默认源，方法如下：

需要创建或修改配置文件（一般都是创建，不同系统配置文件路径见下表），

| 系统 | 路径 |
| --- | --- |
| linux | ~/.pip/pip.conf |
| windows | %HOMEPATH%\pip\pip.ini |

注：windows下可以在cmd中使用 echo %HOMEPATH% 来查看HOMEPATH。

修改内容为：

```
[global]
index-url = http://pypi.douban.com/simple
[install]
trusted-host=pypi.douban.com
```

这样在使用pip来安装时，会默认调用该镜像。

#### 在python脚本中临时使用镜像

临时使用其他源安装软件包的python脚本如下：

```
#!/usr/bin/python

import os

package = input("Input the package:\n")
command = "pip install %s -i http://pypi.mirrors.ustc.edu.cn/simple --trusted-host pypi.mirrors.ustc.edu.cn" % package
os.system(command)
```
