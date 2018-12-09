# flink-tools
flink工作中遇到的开发组件总结
# 项目版本声明
flink ：1.6.0  
scala ：2.11.12  
java ：1.8  

# 描述
  在flink开发中，会遇到一些暂时flink官方未支持的connector，在实际业务场景可能是作为source和sink使用，  
基于业务的需要，自定义实现source和sink。
  项目以rocketmq为例，另外，在笔者的测试中，hbase也可以实现。  
    
  自定义实现source，有两种方式，一种为同时继承RichSourceFunction 和 ParallelSourceFunction，  
  另外笔者在其他的博客中也有人提到直接继承RichParallelSourceFunction，在选择第一种方式时一定要注意别忘记ParallelSourceFunction，
  否则在任务启动后不久就会报出异常，具体的设计模式都是相同的，笔者是以rocketmq为例，读者可以参考类似的结构实现自定义的数据源。
  
  自定义实现sink，一般也有两种方式，项目展示的为第一种较为优雅的方式，继承RichSinkFunction实现函数即可；  
  另外一种方式是写一个sink作用完全相同的函数，作为数据输出使用，然后在数据最后的处理阶段使用map算子对于要发送或写入的数据一条条的处理，
  需要稍微注意的就是函数的对象需要实现序列化，放到main函数外声明，否则会报出 task无法序列化 的错误。
  
  笔者提供了scala和java的两种实现方式，逻辑上是一个思路，对于有些需要scala和java混合开发的读者作为参考。
  

  
