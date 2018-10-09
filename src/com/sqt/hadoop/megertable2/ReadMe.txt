需求：有个订单表，字段有order_id,p_id(产品id),amoun他（数量）。还有个产品表，字段p_id,p_name。将两个表合并
     使其输出的结果为 order_id,p_name,amount.
    //在map阶段合并
    注意:缓存文件的路径不能跟输出目录平级，会报异常
    正确： E:/hadooptest/eache/pd.txt   不能 E:/hadooptest/megertable2/eache/pd.txt
    输出路径:E:/hadooptest/megertable2/output