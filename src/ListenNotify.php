<?php

namespace HugCode\RedisStream;

use HugCode\RedisStream\Contracts\MQTable;

class ListenNotify
{

    /**
     * @var
     */
    private $lastId;

    /**
     * @var
     */
    private $consumer;

    /**
     * @var
     */
    private $group;

    /**
     * @var
     */
    private $readCount = 1;

    /**
     * @var
     */
    private $readSleep;

    public function __construct($config = [])
    {
        if (isset($config['readCount']) && filter_var($config['readCount'], FILTER_VALIDATE_INT)) {
            $this->readCount = $config['readCount'];
        }
        if (isset($config['readSleep']) && filter_var($config['readSleep'], FILTER_VALIDATE_INT)) {
            $this->readSleep = $config['readSleep'];
        }
    }

    /**
     * @Desc
     * @param string $lastId
     * @return $this
     */
    public function setLastID(string $lastId = '')
    {
        $this->lastId = $lastId;
        return $this;
    }

    /**
     * @Desc
     * @method
     * @param string $consumer
     * @param string $group
     * @return $this
     */
    public function setConsumer(string $consumer = '', string $group = '')
    {
        $this->consumer = $consumer;
        $this->group    = $group;
        return $this;
    }

    /**
     * @Desc
     * @param string $stream
     * @param callable $callback
     */
    public function listen(string $stream, callable $callback)
    {
        $stream = new RedisStream($stream);
        if (empty($this->group) || empty($this->consumer)) {
            $this->listenOn($stream, $callback);
        } else {
            $this->listenOn(new RedisConsumer($this->consumer, $stream, $this->group), $callback);
        }
    }

    /**
     * @Desc
     * @param MQTable $on
     * @param callable $callback
     */
    public function listenOn(MQTable $on, callable $callback)
    {
        $lastSeenId = $this->lastId ?? $on->getEntriesKey();
        while (true) {
            $payload = $on->await($lastSeenId, $this->readCount);
            if (!empty($payload)) {
                foreach ($payload[$on->getName()] as $messageId => $data) {
                    $callback($messageId, $data, $on);
                }
            }
            if (!empty($this->readSleep)) {
                sleep($this->readSleep);
            }
        }
    }

}
