<?php

namespace HugCode\RedisStream;

use HugCode\RedisStream\Contracts\MQTable;

class RedisStream implements MQTable
{

    use RedisConnection;

    const ENTRIES_ID = '$';

    private $name;

    /**
     * Stream constructor.
     * @param string $name
     */
    public function __construct(string $name)
    {
        $this->name = $name;
    }

    /**
     * @Desc
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    public function getEntriesKey(): string
    {
        return self::ENTRIES_ID;
    }

    /**
     * @Desc
     * @param array $message
     * @param string $id
     * @param int $maxLen
     * @return string
     */
    public function add(array $message = [], string $id = '*', int $maxLen = 0): string
    {
        return $this->redis()->xAdd($this->name, $id, $message, $maxLen);
    }

    /**
     * @Desc
     * @return int
     */
    public function len(): int
    {
        return $this->redis()->xLen($this->name);
    }

    /**
     * @Desc
     * @param string $id
     * @return int
     */
    public function delete(string $id): int
    {
        return $this->redis()->xDel($this->name, [$id]);
    }

    /**
     * @Desc
     * @param int $maxLen
     * @param bool $isApproximate
     * @return int
     */
    public function trim(int $maxLen = 0, bool $isApproximate = false): int
    {
        return $this->redis()->xTrim($this->name, $maxLen, $isApproximate);
    }

    /**
     * @Desc
     * @param string $from
     * @param int|null $count
     * @return array
     */
    public function read(string $from = '', ?int $count = null): array
    {
        return $this->redis()->xRead([$this->name => $from], $count);
    }

    /**
     * @Desc
     * @param string $lastId
     *          0, ID大于0-0的消息
     *          $, 已经存储的最大ID作为最后一个ID
     * @param null $count
     * @param int $block
     * @return array|null
     */
    public function await(string $lastId = '0', $count = null, $block = null): ?array
    {
        return $this->redis()->xRead([$this->name => $lastId], $count, $block);
    }

    /**
     * @Desc
     * @param string $start
     * @param string $stop
     * @param int|null $count
     * @return array
     */
    public function readRange($start = self::RANGE_FIRST, $stop = self::RANGE_LAST, ?int $count = null): array
    {
        return $this->redis()->xRange($start, $stop, $count);
    }

    /**
     * @Desc
     * @param string $start
     * @param string $stop
     * @param int|null $count
     * @return array
     */
    public function readRevRange($start = self::RANGE_LAST, $stop = self::RANGE_FIRST, ?int $count = null): array
    {
        return $this->redis()->xRevRange($start, $stop, $count);
    }

    /**
     * @Desc
     * @param string $id
     * @return int
     */
    public function ack(string $id): int
    {
        return 0;
    }

}
