使用GraphX自带的PageRank API分析机场与航班关系图
- 航班数据(airports.dat和routes.dat) https://openflights.org/data.html
- 以机场Airport为顶点,以航班Route为边,构建机场之间的联系关系图。两个机场之间的航班数为边的权重, 边是有向的。
- 使用GraphX自带的PageRank API,完成如下分析任务
	- 分别找出中国和美国境内的PageRank值最高的10个机场
	- 以PageRank值降序为标准,找出南京禄口机场在国内和国际的排名。
- 利用机场航班图,利用Spark GraphX完成机场之间的“共同好友数”计算。
	- 四两拨千斤：借助Spark GraphX将QQ千亿关系链计算提速20倍 http://djt.qq.com/article/view/1487