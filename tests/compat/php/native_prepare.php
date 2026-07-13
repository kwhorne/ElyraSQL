<?php
/**
 * Native (binary) prepared-statement regression test for ElyraSQL.
 *
 * Uses PDO with EMULATE_PREPARES = false, so every statement goes through the
 * MySQL binary protocol (COM_STMT_PREPARE / COM_STMT_EXECUTE). This exercises
 * the wire packet framing across *repeated* prepares on one connection, which a
 * use-after-free / buffer-padding bug in the packet reader previously corrupted
 * (mysqlnd: "Wrong COM_STMT_PREPARE response size. Received 7").
 *
 * Connection is read from ELYRASQL_HOST/PORT/USER/PASS (defaults
 * 127.0.0.1:3307 root / no password). The server must run with
 * ELYRASQL_STMT_DESCRIBE=on. Exits non-zero on failure.
 */
$host = getenv('ELYRASQL_HOST') ?: '127.0.0.1';
$port = (int)(getenv('ELYRASQL_PORT') ?: 3307);
$user = getenv('ELYRASQL_USER') ?: 'root';
$pass = getenv('ELYRASQL_PASS') ?: '';

$pdo = new PDO(
    "mysql:host=$host;port=$port;dbname=elyra",
    $user,
    $pass,
    [PDO::ATTR_EMULATE_PREPARES => false, PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
);

$pdo->exec("DROP TABLE IF EXISTS np_b");
$pdo->exec("DROP TABLE IF EXISTS np_a");
$pdo->exec("CREATE TABLE np_a (id INT PRIMARY KEY, name VARCHAR(16))");
$pdo->exec("CREATE TABLE np_b (id INT PRIMARY KEY, a_id INT, label VARCHAR(16))");
$pdo->exec("INSERT INTO np_a VALUES (1,'Ada'),(2,'Lin')");
$pdo->exec("INSERT INTO np_b VALUES (1,1,'post'),(2,2,'blog')");

$pass_n = 0;
$fail_n = 0;
function check($name, $cond, $extra = '')
{
    global $pass_n, $fail_n;
    if ($cond) {
        $pass_n++;
        echo "  ok   $name\n";
    } else {
        $fail_n++;
        echo "  FAIL $name  $extra\n";
    }
}

// A battery of native prepares executed back-to-back on one connection. Before
// the packet-reader fix, the statement after the first row-returning one
// desynced the connection.
$q = function ($sql, $params) use ($pdo) {
    $s = $pdo->prepare($sql);
    $s->execute($params);
    return $s->fetchAll(PDO::FETCH_ASSOC);
};

try {
    $r = $q("SELECT * FROM np_a WHERE id = ?", [1]);
    check("single-table SELECT *", count($r) === 1 && count($r[0]) === 2);

    $r = $q("SELECT * FROM np_a JOIN np_b ON np_b.a_id = np_a.id WHERE np_a.id = ?", [1]);
    check("join SELECT * (5 cols)", count($r) === 1 && count($r[0]) === 5);

    $r = $q("SELECT np_a.name, np_b.label FROM np_a JOIN np_b ON np_b.a_id = np_a.id", []);
    check("join explicit cols", count($r) === 2);

    $r = $q("SELECT * FROM np_a WHERE id = ?", [2]);
    check("repeated prepare", count($r) === 1 && $r[0]['name'] === 'Lin');

    $r = $q("SELECT COUNT(*) c, SUM(id) s FROM np_a", []);
    check("aggregate", (int)$r[0]['c'] === 2 && (int)$r[0]['s'] === 3);

    // parameterised INSERT + SELECT round-trip, natively prepared
    $s = $pdo->prepare("INSERT INTO np_a (id, name) VALUES (?, ?)");
    $s->execute([3, 'Grace']);
    $r = $q("SELECT name FROM np_a WHERE id = ?", [3]);
    check("native INSERT + SELECT", $r[0]['name'] === 'Grace');
} catch (\Throwable $e) {
    check("native prepared statements", false, $e->getMessage());
}

echo "\n$pass_n passed, $fail_n failed\n";
exit($fail_n > 0 ? 1 : 0);
