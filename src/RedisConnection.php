<?php

namespace HugCode\RedisStream;

trait RedisConnection
{

    protected $redis = null;

    protected function redis()
    {
        if (empty($this->redis)) {
            $this->redis = new \Redis();
            $this->redis->connect('192.168.1.10', 6379);
            $this->redis->auth('123456');
            $this->redis->select(0);
        }
        return $this->redis;
    }

}
