<?php

namespace DiBify\Storage\MongoDB\Tests\Fixture;

use Adbar\Dot;
use DiBify\Storage\MongoDB\MongoStorage;
use LogicException;
use MongoDB\Collection;

class TestMongoStorage extends MongoStorage
{
    public function __construct(
        private array $dates = [],
        private array $uuids = [],
        private array $references = [],
        private array $pools = [],
        private array $defaults = [],
        private array $ignore = [],
        private ?string $scope = null,
    ) {
    }

    public function scope(): ?string
    {
        return $this->scope;
    }

    public function scopeKey(): string
    {
        return 'scope';
    }

    public function getCollection(): Collection
    {
        throw new LogicException('getCollection() must not be called in unit tests');
    }

    protected function dates(): array
    {
        return $this->dates;
    }

    protected function uuids(): array
    {
        return $this->uuids;
    }

    protected function references(): array
    {
        return $this->references;
    }

    protected function pools(): array
    {
        return $this->pools;
    }

    protected function defaults(): array
    {
        return $this->defaults;
    }

    protected function ignore(): array
    {
        return $this->ignore;
    }

    public function exposeHandleBeforeWrite(Dot $document, ?callable $handler = null): Dot
    {
        return $this->handleBeforeWrite($document, $handler);
    }

    public function exposeHandleAfterFind(Dot $document): Dot
    {
        return $this->handleAfterFind($document);
    }

    public function exposeDoc2Data($document)
    {
        return $this->doc2data($document);
    }
}
