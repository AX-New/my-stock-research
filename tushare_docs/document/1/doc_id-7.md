## 下载安装

在安装Tushare前，需要提前安装好Python（建议是Python 3.7）环境，我们推荐 安装Anaconda 集成开发环境，并设置好环境变量。

- 方式1： pip install tushare 如果安装网络超时可尝试国内pip源，如pip install tushare -i https://pypi.tuna.tsinghua.edu.cn/simple
- 方式2：访问 https://pypi.python.org/pypi/tushare/ 下载安装 ，执行 python setup.py install
- 方式3：访问 https://github.com/waditu/tushare ,将项目下载或者clone到本地，进入到项目的目录下， 执行： python setup.py install

## 版本升级

```
pip install tushare --upgrade
```

查看当前版本的方法：

```
import tushare

print(tushare.__version__)
```
