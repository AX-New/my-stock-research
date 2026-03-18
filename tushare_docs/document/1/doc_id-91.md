## python过滤HTML标签

作者：Tushare社区用户 晓子

### >> 问题描述 <<

很多时候我们在爬取的网页内容或者提供的资讯数据中包含大量的HTML标签。有些时候，保留这些HTML标签是有用的，例如展现和查看，更多的是为了保留足够多的信息（标签中有但text中没有的信息，如img等）以便后续使用。但有时我们仅仅想要其中的text，这个时候我们需要除去文本中的标签。以下，笔者就介绍两种除去HTML标签的方法。一种使用 BeautifulSoup ，另外一种使用 正则表达式 （这种方法的代码，笔者抄录自网上，由于来源也没有备注作者，所以这里笔者也没有标明作者，如果作者看到这里，请联系晓子 xiaoziwenji@126.com 以正之）。

测试数据来源于tushare pro数字货币交易所Twitter：代码请见：

```
content = pro.exchange_twitter(start_date='2018-08-06 04:16:27', end_date='2018-08-06 04:16:27', fields="content")['content'][0]
```

得到的结果为带有标签的内容：

```
'<span style="color: grey">@joliwa</span> <span style="color: grey">@TurboStakeCoin</span> <span style="color: grey">@Shirt_Fun_Wear</span> Hi there,\nPlease create a support ticket at  <a href="https://t.co/EosnMa5kOP">https://t.co/EosnMa5kOP</a> and our Support Team will be in touch with you soon.'
```

### >> BeautifulSoup <<

使用BeautifulSoup的代码如下：

```
from bs4 import BeautifulSoup

bsObj = BeautifulSoup(content, 'lxml')
bsObj.get_text()
```

结果为：

```
'@joliwa @TurboStakeCoin @Shirt_Fun_Wear Hi there,\nPlease create a support ticket at  https://t.co/EosnMa5kOP and our Support Team will be in touch with you soon.'
```

### >> 正则表达式 <<

同样，笔者也提供了使用正则表达式来过滤标签的代码，方法定义如下：

```
import re

def filter_tags(htmlstr):
    # 先过滤CDATA
    re_cdata = re.compile('//<!\[CDATA\[[^>]*//\]\]>', re.I)  # 匹配CDATA
    re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)  # Script
    re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)  # style
    re_br = re.compile('<br\s*?/?>')  # 处理换行
    re_h = re.compile('</?\w+[^>]*>')  # HTML标签
    re_comment = re.compile('<!--[^>]*-->')  # HTML注释
    s = re_cdata.sub('', htmlstr)  # 去掉CDATA
    s = re_script.sub('', s)  # 去掉SCRIPT
    s = re_style.sub('', s)  # 去掉style
    s = re_br.sub('\n', s)  # 将br转换为换行
    s = re_h.sub('', s)  # 去掉HTML 标签
    s = re_comment.sub('', s)  # 去掉HTML注释
    # 去掉多余的空行
    blank_line = re.compile('\n+')
    s = blank_line.sub('\n', s)
    s = replaceCharEntity(s)  # 替换实体
    return s

def replaceCharEntity(htmlstr):
    CHAR_ENTITIES = {'nbsp': ' ', '160': ' ',
                     'lt': '<', '60': '<',
                     'gt': '>', '62': '>',
                     'amp': '&', '38': '&',
                     'quot': '"', '34': '"', }

    re_charEntity = re.compile(r'&#?(?P<name>\w+);')
    sz = re_charEntity.search(htmlstr)
    while sz:
        entity = sz.group()  # entity全称，如>
        key = sz.group('name')  # 去除&;后entity,如>为gt
        try:
            htmlstr = re_charEntity.sub(CHAR_ENTITIES[key], htmlstr, 1)
            sz = re_charEntity.search(htmlstr)
        except KeyError:
            # 以空串代替
            htmlstr = re_charEntity.sub('', htmlstr, 1)
            sz = re_charEntity.search(htmlstr)
    return htmlstr
```

使用的代码：

```
filter_tags(content)
```

结果为：

```
'@joliwa @TurboStakeCoin @Shirt_Fun_Wear Hi there,\nPlease create a support ticket at  https://t.co/EosnMa5kOP and our Support Team will be in touch with you soon.'
```

### >> 结论 <<

可见两种方法得到的结果是一样的，但使用BeautifulSoup要简单和强大得多。
