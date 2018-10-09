需求：有个订单表，字段有order_id,p_id(产品id),amoun他（数量）。还有个产品表，字段p_id,p_name。将两个表合并
     使其输出的结果为 order_id,p_name,amount.
    //在reduce阶段合并
    1,自定义一个输出结果的TableBean。
    2,自定义一个TableMapper进行读取及相关操作
    3,自定义一个TableReduce进行合并输出