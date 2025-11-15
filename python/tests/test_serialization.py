from dataclasses import dataclass, asdict

import pytest
from scylla.session import Session
from scylla.session_builder import SessionBuilder


@dataclass
class Address:
    """Example UDT class for address"""

    street: str
    city: str
    zip_code: int

    def to_dict(self):
        return {"street": self.street, "city": self.city, "zip_code": self.zip_code}


@dataclass
class Person:
    """Example UDT class for person"""

    name: str
    age: int
    address: Address

    def to_dict(self):
        return {
            "name": self.name,
            "age": self.age,
            "address": self.address.to_dict() if self.address else None,
        }

@pytest.mark.asyncio
async def test_basic_serialization():
    """Test basic type serialization with the new module"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}"
    )
    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
                              CREATE TABLE IF NOT EXISTS basic_types (
                                                                         id int PRIMARY KEY,
                                                                         name text,
                                                                         score double,
                              )
                              """)


    test_values = (1, "Test Name", 95.5)

    result = await session.execute_with_values("INSERT INTO test_ks.basic_types (id, name, score) VALUES (?, ?, ?)", test_values)
    print(f"Basic serialization SUCCESS: {result}")


@pytest.mark.asyncio
async def test_list_serialization():
    """Test list serialization with the new module"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
                              CREATE TABLE IF NOT EXISTS list_types (
                                                                        id int PRIMARY KEY,
                                                                        tags list<text>,
                                                                        scores list<int>
                              )
                              """)


    test_cases = [
        (1, [], []),
        (2, ["tag1"], [100]),
        (3, ["a", "b", "c"], [1, 2, 3, 4, 5]),
        (4, ["test", "with", "spaces"], [0, -1, 999]),
    ]

    for test_values in test_cases:
        result = await session.execute_with_values("INSERT INTO test_ks.list_types (id, tags, scores) VALUES (?, ?, ?)", test_values)
        print(f"List test SUCCESS for {test_values}: {result}")


@pytest.mark.asyncio
async def test_null_values():
    """Test NULL value handling"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
                              CREATE TABLE IF NOT EXISTS nullable_test (
                                                                           id int PRIMARY KEY,
                                                                           name text,
                                                                           score double,
                                                                           tags list<text>
                              )
                              """)


    test_cases = [
        (1, "Not Null", 1.0, ["tag"]),
        (2, None, None, None),
        (3, "Mixed", None, []),
        (4, "Null in list", 2.5, None),
    ]

    for test_values in test_cases:
        result = await session.execute_with_values("INSERT INTO test_ks.nullable_test (id, name, score, tags) VALUES (?, ?, ?, ?)", test_values)
        print(f"NULL test SUCCESS for {test_values}: {result}")


@pytest.mark.asyncio
async def test_structured_serialization():
    """Test the structured serialization approach"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}"
    )
    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
                              CREATE TABLE IF NOT EXISTS structured_test (
                                                                             id int PRIMARY KEY,
                                                                             name text,
                                                                             score double,
                                                                             active boolean,
                                                                             age bigint
                              )
                              """)


    test_values = (1, "Structured Test", 98.5, True, 1234567890)
    result = await session.execute_with_values("INSERT INTO test_ks.structured_test (id, name, score, active, age) VALUES (?, ?, ?, ?, ?)", test_values)
    print(f"Structured serialization SUCCESS: {result}")

    test_values2 = (2, "Another Test", 87.2, False, 9876543210)
    result2 = await session.execute_with_values("INSERT INTO test_ks.structured_test (id, name, score, active, age) VALUES (?, ?, ?, ?, ?)", test_values2)
    print(f"Structured serialization 2 SUCCESS: {result2}")


@pytest.mark.asyncio
async def test_structured_list_serialization():
    """Test structured list serialization"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
                              CREATE TABLE IF NOT EXISTS structured_list_test (
                                                                                  id int PRIMARY KEY,
                                                                                  tags list<text>,
                                                                                  scores list<int>
                              )
                              """)

    test_cases = [
        (1, [], []),
        (2, ["tag1"], [100]),
        (3, ["a", "b", "c"], [1, 2, 3, 4, 5]),
        (4, ["test", "with", "spaces"], [0, -1, 999]),
    ]

    for test_values in test_cases:
        result = await session.execute_with_values("INSERT INTO test_ks.structured_list_test (id, tags, scores) VALUES (?, ?, ?)", test_values)
        print(f"Structured list test SUCCESS for {test_values}: {result}")

@pytest.mark.asyncio
async def test_serialization():
    """Test serialization functionality"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")


    test_values = (
        100,
        "Test User",
        88.8,
    )

    serialization_success = False

    try:
        result = await session.execute_with_values("INSERT INTO test_ks.basic_types (id, name, score) VALUES (?, ?, ?)", test_values)
        print(f"Serialization SUCCESS: {result}")
        serialization_success = True
    except Exception as e:
        print(f"Serialization failed: {e}")

    # Serialization should work
    assert serialization_success, "Serialization failed"

    print(f"Serialization test completed successfully")


@pytest.mark.asyncio
async def test_structured_edge_cases():
    """Test edge cases with structured serialization"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")

    _ = await session.execute("""
                              CREATE TABLE IF NOT EXISTS edge_cases_test (
                                                                             id int PRIMARY KEY,
                                                                             empty_list list<text>,
                                                                             single_item_list list<int>,
                                                                             null_values_list list<text>,
                                                                             mixed_boolean boolean,
                                                                             zero_values bigint,
                                                                             negative_values int
                              )
                              """)

    edge_test_cases = [
        (1, [], [42], [], True, 0, -999),
        (2, [], [], [], False, -1, 0),
        (
            3,
            ["single"],
            [1],
            ["test"],
            True,
            999999999999,
            -2147483648,
        ),
    ]

    for test_values in edge_test_cases:
        try:
            result = await session.execute_with_values("INSERT INTO test_ks.edge_cases_test (id, empty_list, single_item_list, null_values_list, mixed_boolean, zero_values, negative_values) VALUES (?, ?, ?, ?, ?, ?, ?)", test_values)
            print(f"Edge case test SUCCESS for {test_values}: {result}")
        except Exception as e:
            print(f"Edge case test FAILED for {test_values}: {e}")


@pytest.mark.asyncio
async def test_structured_null_handling():
    """Test NULL value handling with structured serialization"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")


    null_test_cases = [
        (10, None, None, None),
        (11, "Test", None, []),
        (12, "Test 2", 85.5, None),
    ]

    for test_values in null_test_cases:
        try:
            result = await session.execute_with_values("INSERT INTO test_ks.nullable_test (id, name, score, tags) VALUES (?, ?, ?, ?)", test_values)
            print(f"NULL handling test SUCCESS for {test_values}: {result}")
        except Exception as e:
            print(f"NULL handling test FAILED for {test_values}: {e}")


@pytest.mark.asyncio
async def test_structured_type_validation():
    """Test type validation errors with structured serialization"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")

    invalid_test_cases = [
        ([1, "test", "not_a_number", True, 123], "Invalid double"),
        ([1, "test", 1.0, "not_a_boolean", 123], "Invalid boolean"),
        (["not_an_int", "test", 1.0, True, 123], "Invalid int"),
        ([1, "test", 1.0, True], "Too few arguments"),
        ([1, "test", 1.0, True, 123, "extra"], "Too many arguments"),
    ]

    for test_values, description in invalid_test_cases:
        try:
            _ = await session.execute_with_values("INSERT INTO test_ks.structured_test (id, name, score, active, age) VALUES (?, ?, ?, ?, ?)", test_values)
            print(f"ERROR: {description} should have failed but didn't")
        except Exception as e:
            print(f"Expected validation error for {description}: {e}")

@pytest.mark.asyncio
async def test_udt_simple():
    # Build a session
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # Ensure keyspace exists and select it
    await session.execute("""
        CREATE KEYSPACE IF NOT EXISTS test_ks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    await session.execute("USE test_ks")

    # Create a clean UDT with one field
    await session.execute("""
        CREATE TYPE IF NOT EXISTS cat (
            name text
        )
    """)

    # Create a table that uses this UDT
    await session.execute("""
                          CREATE TABLE IF NOT EXISTS cats (
                                                              id int PRIMARY KEY,
                                                              info cat
                          )
                          """)


    # --- Insert a dict representation ---
    await session.execute_with_values(
        "INSERT INTO cats (id, info) VALUES (?, ?)",
        (1, {"name": "Whiskers"}),
    )

    # --- Insert a dataclass instance representation ---
    @dataclass
    class Cat:
        name: str

    cat = Cat("Mittens")

    await session.execute_with_values(
        "INSERT INTO cats (id, info) VALUES (?, ?)",
        (2, asdict(cat)),
    )

@pytest.mark.asyncio
async def test_udt():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
        CREATE TYPE IF NOT EXISTS address (
            street text,
            city text,
            zip_code int
        )
    """)
    _ = await session.execute("""
                              CREATE TABLE IF NOT EXISTS users (
                                                                   id int PRIMARY KEY,
                                                                   addr address
                              )
                              """)


    _ = await session.execute_with_values(
        "INSERT INTO users (id, addr) VALUES (?, ?)",
        (
            1,
            {"street": "123 Main St", "city": "Anytown", "zip_code": 12345},
        ),
    )

    @dataclass
    class Address:
        street: str
        city: str
        zip_code: int

    addr = Address("456 Oak Ave", "Springfield", 67890)
#     addr = {
#         "street": "456 Oak Ave",
#         "city": "Springfield",
#         "zip_code": 67890
#     }
    _ = await session.execute_with_values("INSERT INTO users (id, addr) VALUES (?, ?)", (2, asdict(addr)))

@pytest.mark.asyncio
async def test_nested_lists():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    _ = await session.execute("USE test_ks")

    # Tworzymy tabelę z listą zagnieżdżonych list
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS nested_lists_test (
            id int PRIMARY KEY,
            numbers list<frozen<list<int>>>
        );
    """)

    # 1. Wstawiamy zwykłe zagnieżdżone listy Pythonowe
    nested1 = [[1, 2, 3], [4, 5], [], [6]]

    _ = await session.execute_with_values(
        "INSERT INTO nested_lists_test (id, numbers) VALUES (?, ?)",
        (1, nested1),
    )

@pytest.mark.asyncio
async def test_udt_with_lists():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    await session.execute("USE test_ks")

    # Use a unique UDT name to avoid conflicts
    await session.execute("""
        CREATE TYPE IF NOT EXISTS address_test (
            street text,
            city text,
            zip_codes list<int>,
            previous_streets list<text>
        )
    """)
    await session.execute("""
        CREATE TABLE IF NOT EXISTS users_test (
            id int PRIMARY KEY,
            addr frozen<address_test>
        )
    """)

    @dataclass
    class Address:
        street: str
        city: str
        zip_codes: list[int]
        previous_streets: list[str]

    addr = Address("456 Oak Ave", "Springfield", [11111, 22222], ["Elm St"])
    result = await session.execute_with_values(
        "INSERT INTO users_test (id, addr) VALUES (?, ?)",
        (1, asdict(addr))
    )

@pytest.mark.asyncio
async def test_nested_udts():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    await session.execute("USE test_ks")

    inner_udt = "address_inner"
    outer_udt = "person_outer"
    table_name = "people_test"

    await session.execute(f"""
        CREATE TYPE IF NOT EXISTS {inner_udt} (
            street text,
            city text,
            zip_code int
        )
    """)

    await session.execute(f"""
        CREATE TYPE IF NOT EXISTS {outer_udt} (
            name text,
            age int,
            address frozen<{inner_udt}>
        )
    """)

    await session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id int PRIMARY KEY,
            person frozen<{outer_udt}>
        )
    """)

    @dataclass
    class Address:
        street: str
        city: str
        zip_code: int

    @dataclass
    class Person:
        name: str
        age: int
        address: Address

    await session.execute_with_values(
        f"INSERT INTO {table_name} (id, person) VALUES (?, ?)",
        (
            1,
            {
                "name": "Alice",
                "age": 30,
                "address": {"street": "123 Main St", "city": "Anytown", "zip_code": 12345}
            }
        )
    )

    person = Person("Bob", 40, Address("456 Oak Ave", "Springfield", 67890))
    await session.execute_with_values(
        f"INSERT INTO {table_name} (id, person) VALUES (?, ?)",
        (2, asdict(person))
    )

    await session.execute(f"SELECT id, person FROM {table_name} WHERE id IN (1,2)")