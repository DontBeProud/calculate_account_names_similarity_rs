## Introduction

　　如果你从事游戏、APP的开发或运营相关工作，你可能会为黑产团队或游戏工作室的的恶意登录行为感到困扰。面对上百万甚至更多的账号登录记录，当老板要求你尽快从中筛选出哪些是恶意账号而哪些是普通玩家的账号，对此你是否也会感到力不从心？（尤其是面对那些使用动态IP或动态修改机器码的，它们的账号难以通过IP、机器码等信息进行聚类，只能通过账号名称相似度来判断）

　　作为一个懒人，我写了一个相似度聚类脚本以及一个专门为这个场景设计的相似度算法，自动对账号名称进行聚类分析，仅需一杯茶的时间就能揪出高频账号组（如果数据量只有几万的话可能只需撕开调料包的时间），希望能帮到你。

　　If you are engaged in game or APP development or operation related work, you may be troubled by malicious login behaviors of black production teams or game studios. Faced with millions or more of account login records, when your boss asks you to filter out which are malicious accounts and which are ordinary player accounts as soon as possible, will you feel powerless? (Especially for those who use dynamic IP or dynamically modify the machine code, their accounts are difficult to cluster by IP, machine code and other information, and can only be judged by account name similarity)

　　As a lazy person, I wrote a similarity clustering script and a similarity algorithm specially designed for this scenario, which automatically performs cluster analysis on account names. It only takes a cup of tea to find out high-frequency account groups ( If the amount of data is only tens of thousands, it may only take the time to tear the seasoning package), I hope it can help you.


## Installation
```toml
[dependencies]
account_name_similarity = "0.1.0"
```


### Test Data
　　你可以通过下面的链接获取测试数据:

　　You can get the test data by visiting the link below:
    
``` link
链接：https://pan.baidu.com/s/1QOB0143xLmq6UsTSS3XjHg 
提取码：ef7g
```

  
###

### Life is short, why not use python
　　一开始我使用python很轻易地实现了这个功能，但当我第一次使用rust后我爱上了这个语言，所以尝试使用rust重写了这个算法，顺便学习这门语言。由于还是个rust菜鸟，所以有些代码可能写得比较丑陋^_^。当然了，运行效率也有了巨大的提升。由于python可以调用rust，所以旧的python代码就不发上来了。

　　At first, I realized this function easily with python, but when I used rust for the first time, I fell in love with this language, so I tried to use rust to rewrite this algorithm and learn this language by the way. Since I am still a rust rookie, some code may be ugly ^_^. Of course, the operating efficiency has also been greatly improved. Since python can call rust, the old python code will not be posted.
###
