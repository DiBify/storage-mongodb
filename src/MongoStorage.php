<?php
/**
 * Date: 11.04.2020
 * @author Timur Kasumov (XAKEPEHOK)
 */
namespace DiBify\Storage\MongoDB;

use Adbar\Dot;
use DiBify\DiBify\Repository\Storage\StorageData;
use DiBify\DiBify\Repository\Storage\StorageInterface;
use MongoDB\BSON\Binary;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\Cursor;
use RuntimeException;
use XAKEPEHOK\ArrayWildcardExplainer\ArrayWildcardExplainer;

abstract class MongoStorage implements StorageInterface
{

    protected Client $client;

    protected Database $database;

    /** @var StorageData[] */
    protected array $data;

    public static bool $logQueries = false;

    public static array $queries = [];

    public function __construct(Client $client, Database $database)
    {
        $this->client = $client;
        $this->database = $database;
    }

    public function findById(string $id): ?StorageData
    {
        if ($this->scope()) {
            return $this->findOneByFilter(['_id.id' => $id]);
        }
        return $this->findOneByFilter(['_id' => $id]);
    }

    /**
     * @param string[] $ids
     * @return array
     */
    public function findByIds($ids): array
    {
        if (empty($ids)) {
            return [];
        }

        $ids = array_map(fn($id) => (string) $id, $ids);
        if ($this->scope()) {
            $founded = $this->findByFilter(['_id.id' => ['$in' => $ids]]);
        } else {
            $founded = $this->findByFilter(['_id' => ['$in' => $ids]]);
        }

        /**
         * Code bellow needs to sort found models by ids order. Its may be needed in cases of usage multiple
         * storages, like fetching documents ids from elasticsearch storage, and after that fetching actual data
         * from mongodb
         */

        $result = array_flip($ids);
        foreach ($founded as $item) {
            $result[$item->id] = $item;
        }

        return array_filter($result, function ($value) {
            return $value instanceof StorageData;
        });
    }

    public function insert(StorageData $data, array $options = []): void
    {
        $document = new Dot($data->body);

        if ($this->scope()) {
            $document->add("_id.{$this->scopeKey()}", $this->scope());
            $document->add('_id.id', $data->id);
        } else {
            $document->add('_id', $data->id);
        }


        $this->handleBeforeWrite($document);

        $pools = ArrayWildcardExplainer::explainMany($document->all(), $this->pools());
        foreach ($pools as $key) {
            if (($value = $document->get($key)) !== null) {
                $document->set($key, $value['current'] + $value['pool']);
            }
        }

        $this->log('insertOne', $document->all());
        $this->getCollection()->insertOne($document->all(), $options);
    }

    public function update(StorageData $data, array $options = []): void
    {
        $document = new Dot($data->body);

        $inc = [];
        $this->handleBeforeWrite($document, function (Dot $document) use (&$inc) {
            $pools = ArrayWildcardExplainer::explainMany($document->all(), $this->pools());
            foreach ($pools as $key) {
                if (($value = $document->get($key)) !== null) {
                    $inc[$key] = $value['pool'];
                    $document->delete($key);
                }
            }
            return $document;
        });

        if ($this->scope()) {
            $filter = [
                "_id.{$this->scopeKey()}" => $this->scope(),
                '_id.id' => $data->id,
            ];
        } else {
            $filter = [
                '_id' => $data->id,
            ];
        }

        $update = [
            '$set' => $document->all(),
        ];

        if (!empty($inc)) {
            $update['$inc'] = $inc;
        }

        $this->log('updateOne', $filter);
        $this->getCollection()->updateOne($filter, $update, $options);
    }

    public function delete(string $id, array $options = []): void
    {
        $filter = ['_id' => $id];
        if ($this->scope()) {
            $filter =[
                "_id.{$this->scopeKey()}" => $this->scope(),
                '_id.id' => $id,
            ];
        }

        $this->log('delete', $filter);
        $this->getCollection()->deleteOne($filter, $options);
    }

    protected function findOneByFilter(array $filter, array $options = [], bool $withScope = true): ?StorageData
    {
        $options['limit'] = 1;
        $cursor = $this->findByFilter($filter, $options, $withScope);
        $data = current($cursor);
        return $data === false ? null : $data;
    }

    /**
     * @param array $filter
     * @param array $options
     * @param bool $withScope
     * @return StorageData[]
     */
    protected function findByFilter(array $filter, array $options = [], bool $withScope = true): array
    {
        if ($withScope && $this->scope()) {
            $filter = array_merge(["_id.{$this->scopeKey()}" => $this->scope()], $filter);
        }

        $this->log('find', $filter, $options);

        $cursor = $this->getCollection()->find($filter, $options);

        $cursor->setTypeMap(['root' => 'array', 'document' => 'array', 'array' => 'array']);

        $result = [];
        foreach ($cursor as $document) {
            $data = $this->doc2data($document);
            $result[$data->id] = $data;
        }
        return $result;
    }

    protected function countByCondition(array $filter, array $options = [], bool $withScope = true): int
    {
        if ($withScope && $this->scope()) {
            $filter = array_merge(["_id.{$this->scopeKey()}" => $this->scope()], $filter);
        }

        $this->log('countDocuments', $filter, $options);
        return $this->getCollection()->countDocuments($filter, $options);
    }

    protected function aggregate(array $pipeline, array $options = [], bool $withScope = true): Cursor
    {
        if ($withScope && $this->scope()) {
            $pipeline[] = ['$match' => ["_id.{$this->scopeKey()}" => $this->scope()]];
            $pipeline = array_reverse($pipeline, false);
        }

        /** @var Cursor $cursor */
        $cursor = $this->getCollection()->aggregate($pipeline);
        $this->log('aggregate', $pipeline, $options);
        return $cursor;
    }

    protected function defaults(): array
    {
        return [];
    }

    protected function dates(): array
    {
        return [];
    }

    protected function uuids(): array
    {
        return [];
    }

    protected function pools(): array
    {
        return [];
    }

    protected function references(): array
    {
        return [];
    }

    protected function ignore(): array
    {
        return [];
    }

    protected function doc2data($document): StorageData
    {
        $data = (array) $document;

        if ($this->scope()) {
            $id = (string) $data['_id']['id'];

            if ($this->scope() !== $data['_id'][$this->scopeKey()]) {
                throw new RuntimeException("Storage return data for other scope");
            }
        } else {
            $id = (string) $data['_id'];
        }

        unset($data['_id']);
        return new StorageData($id, $this->handleAfterFind(new Dot($data))->all(), $this->scope());
    }

    protected function handleAfterFind(Dot $document): Dot
    {
        foreach ($this->defaults() as $key => $value) {
            if (!$document->has($key)) {
                $document->set($key, $value);
            }
        }

        $dates = ArrayWildcardExplainer::explainMany($document->all(), $this->dates());
        foreach ($dates as $key) {
            /** @var UTCDateTime $datetime */
            if (($datetime = $document->get($key)) !== null) {
                $document->set($key, (int) $datetime->toDateTime()->format('U'));
            }
        }

        $uuids = ArrayWildcardExplainer::explainMany($document->all(), array_merge(
            $this->uuids(),
            array_keys($this->references())
        ));

        foreach ($uuids as $key) {
            /** @var Binary $uuid */
            if (($uuid = $document->get($key)) !== null) {
                if ($uuid instanceof Binary) {
                    $uuid = preg_replace(
                        '~(\w{8})(\w{4})(\w{4})(\w{4})(\w{12})~',
                        '$1-$2-$3-$4-$5',
                        bin2hex($uuid->getData())
                    );
                    $document->set($key, $uuid);
                }
            }
        }

        $pools = ArrayWildcardExplainer::explainMany($document->all(), $this->pools());
        foreach ($pools as $poolKey) {
            $value = $document->get($poolKey);
            if (is_scalar($value)) {
                $document->set($poolKey, [
                    'current' => $value,
                    'pool' => 0
                ]);
            }
        }

        $references = $this->expandReferences($document);
        foreach ($references as $key => $alias) {
            if (($id = $document->get($key)) !== null) {
                $document->set($key, ['alias' => $alias, 'id' => $id]);
            }
        }

        $ignore = ArrayWildcardExplainer::explainMany($document->all(), $this->ignore());
        foreach ($ignore as $key) {
            $document->delete($key);
        }

        $data = json_decode(json_encode($document), true);

        return new Dot($data);
    }

    protected function handleBeforeWrite(Dot $document, callable $handler = null): Dot
    {
        if (is_null($handler)) {
            $handler = fn($value) => $value;
        }

        $dates = ArrayWildcardExplainer::explainMany($document->all(), $this->dates());
        foreach ($dates as $key) {
            if (($timestamp = $document->get($key)) !== null) {
                $document->set($key, new UTCDateTime((int) $timestamp * 1000));
            }
        }

        $references = $this->expandReferences($document);
        foreach ($references as $key => $alias) {
            if (($reference = $document->get($key)) !== null) {
                $document->set($key, $reference['id']);
            }
        }

        $uuids = ArrayWildcardExplainer::explainMany($document->all(), $this->uuids());
        foreach ($uuids as $key) {
            if (($uuid = $document->get($key)) !== null) {
                if (preg_match('~^[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}$~i', $uuid)) {
                    $uuid = str_replace('-', '', $uuid);
                    $document->set($key, new Binary(hex2bin($uuid), Binary::TYPE_UUID));
                }
            }
        }

        $ignore = ArrayWildcardExplainer::explainMany($document->all(), $this->ignore());
        foreach ($ignore as $key) {
            $document->delete($key);
        }

        return $handler($document);
    }

    protected function expandReferences(Dot $document): array
    {
        $references = [];
        foreach ($this->references() as $key => $alias) {
            $expanded = ArrayWildcardExplainer::explainOne($document->all(), $key);
            foreach ($expanded as $value) {
                $references[$value] = $alias;
            }
        }
        return $references;
    }

    protected function log(string $type, array $filter = [], array $options = [])
    {
        if (!self::$logQueries) {
            return;
        }

        $command = "db.{$this->getCollection()->getCollectionName()}.{$type}";
        $filter = json_encode($filter, JSON_UNESCAPED_UNICODE & JSON_UNESCAPED_SLASHES & JSON_UNESCAPED_UNICODE);

        $chain = ["$command({$filter})"];
        foreach ($options as $option => $values) {
            $chain[] = "{$option}(" . json_encode($values, JSON_UNESCAPED_UNICODE & JSON_UNESCAPED_SLASHES & JSON_UNESCAPED_UNICODE) . ")";
        }

        self::$queries[] = implode('.', $chain);
    }

    abstract public function scope(): ?string;

    abstract public function scopeKey(): string;

    abstract public function getCollection(): Collection;

    public function freeUpMemory(): void
    {
        $this->data = [];
        self::$queries = [];
    }

}