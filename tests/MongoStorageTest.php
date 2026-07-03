<?php

namespace DiBify\Storage\MongoDB\Tests;

use Adbar\Dot;
use DiBify\Storage\MongoDB\Tests\Fixture\TestMongoStorage;
use MongoDB\BSON\Binary;
use MongoDB\BSON\UTCDateTime;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class MongoStorageTest extends TestCase
{
    // Write: timestamp (sec) -> UTCDateTime (ms)
    public function testDateIsConvertedToUtcDateTimeOnWrite(): void
    {
        $storage = new TestMongoStorage(dates: ['createdAt']);

        $result = $storage->exposeHandleBeforeWrite(new Dot(['createdAt' => 1700000000]));

        $this->assertInstanceOf(UTCDateTime::class, $result->get('createdAt'));
        // UTCDateTime stores milliseconds
        $this->assertSame('1700000000000', (string)$result->get('createdAt'));
    }

    // Read: UTCDateTime (ms) -> timestamp (sec)
    public function testDateIsConvertedBackToTimestampOnRead(): void
    {
        $storage = new TestMongoStorage(dates: ['createdAt']);

        $result = $storage->exposeHandleAfterFind(new Dot([
            'createdAt' => new UTCDateTime(1700000000000),
        ]));

        $this->assertSame(1700000000, $result->get('createdAt'));
    }

    // UUID string -> Binary(TYPE_UUID) on write and back to string on read
    public function testUuidRoundTrip(): void
    {
        $storage = new TestMongoStorage(uuids: ['token']);
        $uuid = '00112233-4455-6677-8899-aabbccddeeff';

        $written = $storage->exposeHandleBeforeWrite(new Dot(['token' => $uuid]));
        $this->assertInstanceOf(Binary::class, $written->get('token'));
        $this->assertSame(Binary::TYPE_UUID, $written->get('token')->getType());

        $read = $storage->exposeHandleAfterFind(new Dot(['token' => $written->get('token')]));
        $this->assertSame($uuid, $read->get('token'));
    }

    // Reference: ['alias','id'] -> bare id on write, id -> ['alias','id'] on read
    public function testReferenceIsFlattenedOnWriteAndExpandedOnRead(): void
    {
        $storage = new TestMongoStorage(references: ['author' => 'user']);

        $written = $storage->exposeHandleBeforeWrite(new Dot([
            'author' => ['alias' => 'user', 'id' => '42'],
        ]));
        $this->assertSame('42', $written->get('author'));

        $read = $storage->exposeHandleAfterFind(new Dot(['author' => '42']));
        $this->assertSame(['alias' => 'user', 'id' => '42'], $read->get('author'));
    }

    // Pool: scalar -> ['current' => value, 'pool' => 0] on read
    public function testPoolScalarIsExpandedOnRead(): void
    {
        $storage = new TestMongoStorage(pools: ['balance']);

        $read = $storage->exposeHandleAfterFind(new Dot(['balance' => 100]));

        $this->assertSame(['current' => 100, 'pool' => 0], $read->get('balance'));
    }

    // Default is applied on read when the field is missing
    public function testDefaultsAreAppliedOnReadWhenMissing(): void
    {
        $storage = new TestMongoStorage(defaults: ['status' => 'active']);

        $read = $storage->exposeHandleAfterFind(new Dot(['name' => 'John']));

        $this->assertSame('active', $read->get('status'));
    }

    // Default does not override an already-existing value
    public function testDefaultsDoNotOverrideExistingValue(): void
    {
        $storage = new TestMongoStorage(defaults: ['status' => 'active']);

        $read = $storage->exposeHandleAfterFind(new Dot(['status' => 'banned']));

        $this->assertSame('banned', $read->get('status'));
    }

    // Fields listed in ignore() are stripped on write
    public function testIgnoredFieldsAreStrippedOnWrite(): void
    {
        $storage = new TestMongoStorage(ignore: ['secret']);

        $written = $storage->exposeHandleBeforeWrite(new Dot([
            'name' => 'John',
            'secret' => 'hidden',
        ]));

        $this->assertFalse($written->has('secret'));
        $this->assertSame('John', $written->get('name'));
    }

    // (1) Nested default, parent = null -> stays null, no partial is created
    public function testNestedDefaultDoesNotMaterializeNullParent(): void
    {
        $storage = new TestMongoStorage(defaults: ['logistic.shippedAt' => null]);

        $read = $storage->exposeHandleAfterFind(new Dot(['logistic' => null]));

        $this->assertNull($read->get('logistic'));
        $this->assertFalse($read->has('logistic.shippedAt'));
    }

    // (2) Nested default, parent = scalar/string -> is not turned into an array
    public function testNestedDefaultDoesNotMaterializeScalarParent(): void
    {
        $storage = new TestMongoStorage(defaults: ['logistic.shippedAt' => null]);

        $read = $storage->exposeHandleAfterFind(new Dot(['logistic' => 'raw-string']));

        $this->assertSame('raw-string', $read->get('logistic'));
    }

    // (3) Nested default, parent is entirely absent -> not created
    public function testNestedDefaultDoesNotMaterializeAbsentParent(): void
    {
        $storage = new TestMongoStorage(defaults: ['logistic.shippedAt' => null]);

        $read = $storage->exposeHandleAfterFind(new Dot(['name' => 'John']));

        $this->assertFalse($read->has('logistic'));
    }

    // (4) Nested default, parent = existing array without the leaf -> leaf is set (regression guard)
    public function testNestedDefaultIsAppliedWhenParentIsExistingArray(): void
    {
        $storage = new TestMongoStorage(defaults: ['logistic.shippedAt' => null]);

        $read = $storage->exposeHandleAfterFind(new Dot([
            'logistic' => ['plugin' => 'dhl'],
        ]));

        $this->assertSame(['plugin' => 'dhl', 'shippedAt' => null], $read->get('logistic'));
    }

    // (5) Flat default, key missing -> applied
    public function testFlatDefaultIsAppliedWhenMissing(): void
    {
        $storage = new TestMongoStorage(defaults: ['status' => 'active']);

        $read = $storage->exposeHandleAfterFind(new Dot(['name' => 'John']));

        $this->assertSame('active', $read->get('status'));
    }

    // (6) Default does not override an already-existing value (nested too)
    public function testDefaultDoesNotOverrideExistingNestedValue(): void
    {
        $storage = new TestMongoStorage(defaults: ['logistic.shippedAt' => null]);

        $read = $storage->exposeHandleAfterFind(new Dot([
            'logistic' => ['shippedAt' => 12345],
        ]));

        $this->assertSame(12345, $read->get('logistic.shippedAt'));
    }

    // (7) Multiple defaults under the same null parent -> parent stays null
    public function testMultipleNestedDefaultsDoNotMaterializeNullParent(): void
    {
        $storage = new TestMongoStorage(defaults: [
            'logistic.shippedAt' => null,
            'logistic.deliveredAt' => null,
        ]);

        $read = $storage->exposeHandleAfterFind(new Dot(['logistic' => null]));

        $this->assertNull($read->get('logistic'));
    }

    // (8) A leading "!" forces materialization of a null parent; the "!" is stripped from the key
    public function testBangPrefixForcesMaterializationOfNullParent(): void
    {
        $storage = new TestMongoStorage(defaults: ['!logistic.shippedAt' => null]);

        $read = $storage->exposeHandleAfterFind(new Dot(['logistic' => null]));

        $this->assertSame(['shippedAt' => null], $read->get('logistic'));
        $this->assertFalse($read->has('!logistic')); // "!" never reaches the document
    }

    // (8b) A leading "!" on a flat key simply applies the default with the "!" stripped
    public function testBangPrefixOnFlatKeyStripsBang(): void
    {
        $storage = new TestMongoStorage(defaults: ['!status' => 'active']);

        $read = $storage->exposeHandleAfterFind(new Dot(['name' => 'John']));

        $this->assertSame('active', $read->get('status'));
        $this->assertFalse($read->has('!status'));
    }

    // (8c) A leading "!" still does not override an already-existing value
    public function testBangPrefixDoesNotOverrideExistingValue(): void
    {
        $storage = new TestMongoStorage(defaults: ['!logistic.shippedAt' => null]);

        $read = $storage->exposeHandleAfterFind(new Dot([
            'logistic' => ['shippedAt' => 12345],
        ]));

        $this->assertSame(12345, $read->get('logistic.shippedAt'));
    }

    // doc2data without scope: _id -> id, the rest -> body
    public function testDoc2DataExtractsIdWithoutScope(): void
    {
        $storage = new TestMongoStorage();

        $data = $storage->exposeDoc2Data(['_id' => 'abc', 'name' => 'John']);

        $this->assertSame('abc', $data->id);
        $this->assertSame(['name' => 'John'], $data->body);
        $this->assertNull($data->scope);
    }

    // doc2data with scope: _id.{scope,id} -> id + scope, the rest -> body
    public function testDoc2DataExtractsIdWithScope(): void
    {
        $storage = new TestMongoStorage(scope: 'tenant-1');

        $data = $storage->exposeDoc2Data([
            '_id' => ['scope' => 'tenant-1', 'id' => 'abc'],
            'name' => 'John',
        ]);

        $this->assertSame('abc', $data->id);
        $this->assertSame('tenant-1', $data->scope);
        $this->assertSame(['name' => 'John'], $data->body);
    }

    // doc2data throws if a document of a foreign scope comes in
    public function testDoc2DataRejectsForeignScope(): void
    {
        $storage = new TestMongoStorage(scope: 'tenant-1');

        $this->expectException(RuntimeException::class);
        $storage->exposeDoc2Data([
            '_id' => ['scope' => 'tenant-2', 'id' => 'abc'],
            'name' => 'John',
        ]);
    }
}
