<?php

namespace HugCode\RedisStream;

use HugCode\RedisStream\Contracts\MQTable;

class RedisConsumer implements MQTable
{

    use RedisConnection;

    const ENTRIES_ID = '>';

    /**
     * @var string
     */
    private $consumer;

    /**
     * @var RedisStream
     */
    private $stream;

    /**
     * @var string
     */
    private $group;

    /**
     * Consumer constructor.
     *
     * @param string $consumer
     * @param RedisStream $stream
     * @param string $group
     */
    public function __construct(string $consumer, RedisStream $stream, string $group)
    {
        $this->stream   = $stream;
        $this->group    = $group;
        $this->consumer = $consumer;
    }

    public function getEntriesKey(): string
    {
        return self::ENTRIES_ID;
    }

    /**
     * @Desc
     * @return string
     */
    public function getName(): string
    {
        return $this->stream->getName();
    }

    /**
     * @Desc
     * @param string $name
     * @param string $from
     *              0, 消费者组将会开始消费这个Stream中的所有历史消息
     *              $, 只有从现在开始到达Stream的新消息才会被传递到消费者组中的消费者
     * @param bool|string $createStream
     * @return mixed
     */
    public function createGroup(string $name = '', string $from = '0', $createStream = false)
    {
        $name = !empty($name) ? $name : $this->group;
        return $this->redis()->xGroup('CREATE', $this->stream->getName(), $name, $from, $createStream);
    }

    /**
     * @Desc
     * @param string $lastId
     *               >, 消息到目前为止从未传递给其他消费者
     *          消息 id, 历史待处理消息
     * @param int $block
     * @param null $count
     * @return array
     */
    public function await(string $lastId = self::ENTRIES_ID, $count = null, $block = null): array
    {
        $result = $this->redis()->xReadGroup(
            $this->group, $this->consumer, [$this->stream->getName() => $lastId], $count, $block
        );
        return !is_array($result) ? [] : $result;
    }

    /**
     * @Desc
     * @param string $id
     * @return int
     */
    public function ack(string $id): int
    {
        return $this->redis()->xAck($this->stream->getName(), $this->group, [$id]);
    }

    /**
     * @Desc
     * @param string $group
     * @param string $start
     * @param string $end
     * @param null $count
     * @param null $consumer
     * @return array
     */
    public function pending($group = '', $start = self::RANGE_FIRST, $end = self::RANGE_LAST, $count = null, $consumer = null)
    {
        $group = !empty($group) ? $group : $this->group;
        if (empty($count)) {
            $pending = $this->redis()->xPending($this->stream->getName(), $group);
            $count   = array_shift($pending);
        }

        if ($consumer) {
            return $this->redis()->xPending($this->stream->getName(), $group, $start, $end, $count, $consumer);
        }
        return $this->redis()->xPending($this->stream->getName(), $group, $start, $end, $count);
    }

    /**
     * @Desc
     * @param array $ids
     * @param int $idleTime
     * @param bool $justId
     * @return array
     */
    public function claim(array $ids, int $idleTime, $justId = true): array
    {
        if ($justId) {
            return $this->redis()->xClaim($this->stream->getName(), $this->group, $this->consumer, $idleTime, $ids, ['JUSTID']);
        }
        return $this->redis()->xClaim($this->stream->getName(), $this->group, $this->consumer, $idleTime, $ids);
    }

    /**
     * @Desc
     * @param string $key
     * @return mixed
     */
    public function infoStream(string $key = '')
    {
        return $this->redis()->xInfo('STREAM', $key);
    }

    /**
     * @Desc
     * @param string $group
     * @return mixed
     */
    public function infoGroups(string $group = '')
    {
        return $this->redis()->xInfo('GROUPS', $group);
    }

    /**
     * @Desc
     * @param string $group
     * @return mixed
     */
    public function infoConsumers(string $group = '')
    {
        return $this->redis()->xInfo('CONSUMERS', $this->stream->getName(), $group);
    }

}
