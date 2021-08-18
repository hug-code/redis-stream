<?php

namespace HugCode\RedisStream\Contracts;

interface MQTable
{

    const RANGE_FIRST = '-';
    const RANGE_LAST = '+';

    public function await(string $lastId = '', $count = null, $block = null): ?array;

    public function getEntriesKey(): string;

    public function getName(): string;

    public function ack(string $id): int;

}
