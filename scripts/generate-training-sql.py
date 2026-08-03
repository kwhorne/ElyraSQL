#!/usr/bin/env python3
"""Generate training SQL dumps for PGO profile generation.

Each "model" creates a schema with varied column types and
constraints, inserts representative data, and runs queries
that exercise different code paths in the query engine.

The generated SQL is written to --output-dir (one .sql file
per model) so pgo-build.sh can feed them to the instrumented
server during training.
"""

import argparse
import os
import sys

MODELS = [
    ("scalar_matrix", 50000),
    ("relational_graph", 50000),
    ("commerce_graph", 50000),
    ("car_dealership", 10000),
]


def gen_scalar_matrix(path, rows):
    """Wide table with many scalar types — exercises type coercion, encoding, comparisons."""
    with open(path, "w") as f:
        f.write("CREATE TABLE scalar_matrix (\n")
        f.write("  id BIGINT PRIMARY KEY,\n")
        f.write("  tiny TINYINT,\n")
        f.write("  short SMALLINT,\n")
        f.write("  med MEDIUMINT,\n")
        f.write("  big BIGINT,\n")
        f.write("  dec_val DECIMAL(18, 4),\n")
        f.write("  fl FLOAT,\n")
        f.write("  dbl DOUBLE,\n")
        f.write("  txt VARCHAR(255),\n")
        f.write("  flag BOOLEAN,\n")
        f.write("  ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n")
        f.write("  dt DATE,\n")
        f.write("  bin BLOB\n")
        f.write(");\n")
        for i in range(1, rows + 1):
            if i % 1000 == 1:
                f.write(f"INSERT INTO scalar_matrix (id,tiny,short,med,big,dec_val,fl,dbl,txt,flag,dt,bin) VALUES\n")
            comma = "," if i % 1000 != 0 and i != rows else ";"
            f.write(
                f"  ({i},{i % 128},{i * 10 % 32767},{i * 100},{i * 1000000},"
                f"{i * 123.4567:.4f},{i * 1.5},{i * 3.14159},"
                f"'row_{i}text',{'TRUE' if i % 2 == 1 else 'FALSE'},"
                f"DATE_ADD('2020-01-01', INTERVAL {i % 365} DAY),"
                f"X'{i:08x}'){comma}\n"
            )
        f.write("SELECT COUNT(*) FROM scalar_matrix;\n")
        f.write("SELECT AVG(dbl), SUM(med), MIN(big), MAX(dec_val) FROM scalar_matrix;\n")
        f.write("SELECT txt, COUNT(*) FROM scalar_matrix GROUP BY id % 10;\n")
        f.write("SELECT * FROM scalar_matrix WHERE fl > 100 ORDER BY id LIMIT 500;\n")
        f.write("SELECT * FROM scalar_matrix WHERE txt LIKE 'row_1%' LIMIT 100;\n")


def gen_relational_graph(path, rows):
    """Two related tables — exercises foreign keys, joins, indexed lookups."""
    with open(path, "w") as f:
        f.write("CREATE TABLE users (\n")
        f.write("  id BIGINT PRIMARY KEY,\n")
        f.write("  name VARCHAR(100) NOT NULL,\n")
        f.write("  email VARCHAR(200),\n")
        f.write("  age INT,\n")
        f.write("  score DECIMAL(10, 2),\n")
        f.write("  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n")
        f.write(");\n")
        f.write("CREATE TABLE orders (\n")
        f.write("  id BIGINT PRIMARY KEY,\n")
        f.write("  user_id BIGINT NOT NULL,\n")
        f.write("  amount DECIMAL(12, 2),\n")
        f.write("  status VARCHAR(20),\n")
        f.write("  placed_at TIMESTAMP,\n")
        f.write("  FOREIGN KEY (user_id) REFERENCES users(id)\n")
        f.write(");\n")

        for i in range(1, rows + 1):
            if i % 1000 == 1:
                f.write(f"INSERT INTO users (id,name,email,age,score) VALUES\n")
            comma = "," if i % 1000 != 0 and i != rows else ";"
            f.write(f"  ({i},'user_{i}','user{i}@example.com',{20 + i % 50},{i * 10.5:.2f}){comma}\n")

        for i in range(1, rows + 1):
            if i % 1000 == 1:
                f.write(f"INSERT INTO orders (id,user_id,amount,status,placed_at) VALUES\n")
            comma = "," if i % 1000 != 0 and i != rows else ";"
            st = ["pending", "shipped", "delivered", "cancelled"][i % 4]
            f.write(f"  ({i},{1 + i % rows},{i * 99.99:.2f},'{st}',NOW()){comma}\n")

        f.write("SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY SUM(o.amount) DESC LIMIT 100;\n")
        f.write("SELECT * FROM users WHERE age BETWEEN 30 AND 40 ORDER BY score DESC LIMIT 50;\n")
        f.write("SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'delivered' GROUP BY u.id LIMIT 50;\n")
        f.write("SELECT * FROM orders WHERE amount > 500 ORDER BY placed_at LIMIT 100;\n")


def gen_commerce_graph(path, rows):
    """Three tables with many-to-many — exercises multi-join, aggregation, subquery patterns."""
    with open(path, "w") as f:
        f.write("CREATE TABLE products (\n")
        f.write("  id BIGINT PRIMARY KEY,\n")
        f.write("  name VARCHAR(200),\n")
        f.write("  category_id INT,\n")
        f.write("  price DECIMAL(10, 2),\n")
        f.write("  weight DOUBLE,\n")
        f.write("  in_stock BOOLEAN DEFAULT TRUE\n")
        f.write(");\n")
        f.write("CREATE TABLE customers (\n")
        f.write("  id BIGINT PRIMARY KEY,\n")
        f.write("  name VARCHAR(100),\n")
        f.write("  region VARCHAR(50),\n")
        f.write("  tier TINYINT DEFAULT 1\n")
        f.write(");\n")
        f.write("CREATE TABLE purchases (\n")
        f.write("  id BIGINT PRIMARY KEY,\n")
        f.write("  customer_id BIGINT NOT NULL,\n")
        f.write("  product_id BIGINT NOT NULL,\n")
        f.write("  quantity INT,\n")
        f.write("  total DECIMAL(12, 2),\n")
        f.write("  purchased_at TIMESTAMP,\n")
        f.write("  FOREIGN KEY (customer_id) REFERENCES customers(id),\n")
        f.write("  FOREIGN KEY (product_id) REFERENCES products(id)\n")
        f.write(");\n")

        for i in range(1, rows + 1):
            if i % 1000 == 1:
                f.write(f"INSERT INTO products (id,name,category_id,price,weight) VALUES\n")
            comma = "," if i % 1000 != 0 and i != rows else ";"
            f.write(f"  ({i},'product_{i}',{i % 100},{i * 49.99:.2f},{i * 0.75}){comma}\n")

        cust_rows = max(rows // 10, 1000)
        for i in range(1, cust_rows + 1):
            if i % 1000 == 1:
                f.write(f"INSERT INTO customers (id,name,region,tier) VALUES\n")
            comma = "," if i % 1000 != 0 and i != cust_rows else ";"
            f.write(f"  ({i},'cust_{i}','region_{i % 10}',{1 + i % 3}){comma}\n")

        for i in range(1, rows + 1):
            if i % 1000 == 1:
                f.write(f"INSERT INTO purchases (id,customer_id,product_id,quantity,total,purchased_at) VALUES\n")
            comma = "," if i % 1000 != 0 and i != rows else ";"
            f.write(f"  ({i},{1 + i % cust_rows},{1 + i % rows},{i % 5 + 1},{i * 49.99 * (i % 5 + 1):.2f},NOW()){comma}\n")

        f.write("SELECT p.name, SUM(pu.total) FROM products p JOIN purchases pu ON p.id = pu.product_id GROUP BY p.id ORDER BY SUM(pu.total) DESC LIMIT 100;\n")
        f.write("SELECT c.region, SUM(pu.total) FROM customers c JOIN purchases pu ON c.id = pu.customer_id GROUP BY c.region;\n")
        f.write("SELECT c.name, p.name, pu.quantity FROM customers c JOIN purchases pu ON c.id = pu.customer_id JOIN products p ON p.id = pu.product_id WHERE pu.quantity > 3 ORDER BY pu.total DESC LIMIT 100;\n")
        f.write("SELECT category_id, AVG(price), COUNT(*) FROM products GROUP BY category_id HAVING COUNT(*) > 10 ORDER BY AVG(price) DESC;\n")


def gen_car_dealership(path, rows):
    """Single rich table — exercises text search, range scans, complex predicates."""
    with open(path, "w") as f:
        f.write("CREATE TABLE cars (\n")
        f.write("  id BIGINT PRIMARY KEY,\n")
        f.write("  make VARCHAR(50),\n")
        f.write("  model VARCHAR(100),\n")
        f.write("  year INT,\n")
        f.write("  color VARCHAR(30),\n")
        f.write("  mileage INT,\n")
        f.write("  price DECIMAL(10, 2),\n")
        f.write("  vin VARCHAR(17),\n")
        f.write("  transmission VARCHAR(20),\n")
        f.write("  fuel VARCHAR(20),\n")
        f.write("  listed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n")
        f.write(");\n")

        makes = ["Toyota", "Honda", "Ford", "BMW", "Mercedes", "Audi", "Tesla", "Hyundai", "Kia", "Mazda"]
        colors = ["Red", "Blue", "Black", "White", "Silver", "Green", "Gray"]
        transmissions = ["Automatic", "Manual", "CVT"]
        fuels = ["Gasoline", "Diesel", "Electric", "Hybrid"]

        for i in range(1, rows + 1):
            if i % 1000 == 1:
                f.write(f"INSERT INTO cars (id,make,model,year,color,mileage,price,vin,transmission,fuel) VALUES\n")
            comma = "," if i % 1000 != 0 and i != rows else ";"
            mk = makes[i % len(makes)]
            f.write(
                f"  ({i},'{mk}','{mk}_{(i % 10) + 1}',"
                f"{2010 + i % 16},'{colors[i % len(colors)]}',"
                f"{i * 1000},{10000 + i * 500:.2f},"
                f"'VIN{i:013d}','{transmissions[i % len(transmissions)]}','{fuels[i % len(fuels)]}'){comma}\n"
            )

        f.write("SELECT make, COUNT(*), AVG(price) FROM cars GROUP BY make ORDER BY AVG(price) DESC;\n")
        f.write("SELECT * FROM cars WHERE price BETWEEN 20000 AND 40000 AND year >= 2018 ORDER BY mileage ASC LIMIT 200;\n")
        f.write("SELECT make, model, COUNT(*) FROM cars WHERE fuel = 'Electric' GROUP BY make, model ORDER BY COUNT(*) DESC LIMIT 20;\n")
        f.write("SELECT color, AVG(mileage), MIN(price), MAX(price) FROM cars WHERE year >= 2015 GROUP BY color;\n")
        f.write("SELECT * FROM cars WHERE make = 'Toyota' AND color = 'White' ORDER BY price LIMIT 50;\n")


GENERATORS = {
    "scalar_matrix": gen_scalar_matrix,
    "relational_graph": gen_relational_graph,
    "commerce_graph": gen_commerce_graph,
    "car_dealership": gen_car_dealership,
}


def main():
    ap = argparse.ArgumentParser(description="Generate training SQL dumps for PGO")
    ap.add_argument("--output-dir", default="target/pgo/training-sql", help="Output directory for .sql files")
    ap.add_argument("--all", action="store_true", help="Generate all models")
    ap.add_argument("--model", help="Generate a specific model")
    ap.add_argument("--rows", type=int, default=50000, help="Rows per model (overrides defaults)")
    ap.add_argument("--list", action="store_true", help="List available models")
    args = ap.parse_args()

    if args.list:
        for name in GENERATORS:
            print(name)
        return

    os.makedirs(args.output_dir, exist_ok=True)

    if args.all:
        models = [(m, args.rows) for m, _ in MODELS]
    elif args.model:
        if args.model not in GENERATORS:
            print(f"Unknown model: {args.model}", file=sys.stderr)
            sys.exit(1)
        models = [(args.model, args.rows)]
    else:
        models = MODELS

    for name, rows in models:
        path = os.path.join(args.output_dir, f"{name}.sql")
        print(f"Generating {name} ({rows} rows) -> {path} ...")
        gen = GENERATORS[name]
        gen(path, rows)
        size_kb = os.path.getsize(path) / 1024
        print(f"  {size_kb:.0f} KB written")

    print(f"\nDone. {len(models)} SQL file(s) in {args.output_dir}/")


if __name__ == "__main__":
    main()
