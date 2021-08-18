# RedisStream
> redis stream 消息队列

#### 配置与方法调用 (参考)
```php
// 发送消息
$streamName = 'my_stream';
$stream = new hugCode\RedisStream\RedisStream($streamName);
$messageId = $stream->add([
    'content' => 'qqq',
    'name' => time()
]);


// 创建分组
$consumer = new hugCode\RedisStream\RedisConsumer('my_consumer', $stream, 'my_group');
$consumer->createGroup();

// 监听
$ListenNotify = new hugCode\RedisStream\ListenNotify();
$ListenNotify->setConsumer('my_consumer', 'my_group')->listen($streamName, function ($msgId, $data, hugCode\RedisStream\Contracts\MQTable $on) {
    var_dump($msgId, $data);
    $on->ack($msgId); // 如果没有创建消费者组，ack方法无效
});
```
