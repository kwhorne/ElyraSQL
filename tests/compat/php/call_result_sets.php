<?php
/**
 * CALL result sets through PDO, as a Laravel application reads them.
 *
 * A procedure's SELECTs come back as consecutive result sets, which PDO walks
 * with nextRowset(), then the CALL's own status. Checked twice: with emulated
 * prepares (text protocol, Laravel's default) and native ones (binary
 * protocol). Afterwards the connection must accept the next statement.
 *
 * Connection is read from ELYRASQL_HOST/PORT/USER/PASS/DB (defaults
 * 127.0.0.1:3307 root / no password, database elyra). Exits non-zero on failure.
 */
$host = getenv('ELYRASQL_HOST') ?: '127.0.0.1';
$port = (int)(getenv('ELYRASQL_PORT') ?: 3307);
$user = getenv('ELYRASQL_USER') ?: 'root';
$pass = getenv('ELYRASQL_PASS') ?: '';
$db = getenv('ELYRASQL_DB') ?: 'elyra';

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

function connect($emulate)
{
    global $host, $port, $user, $pass, $db;
    return new PDO(
        "mysql:host=$host;port=$port;dbname=$db",
        $user,
        $pass,
        [PDO::ATTR_EMULATE_PREPARES => $emulate, PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
    );
}

/** Every result set of an executed statement, as arrays of assoc rows. */
function rowsets($stmt)
{
    $sets = [];
    do {
        if ($stmt->columnCount() > 0) {
            $sets[] = $stmt->fetchAll(PDO::FETCH_ASSOC);
        }
    } while ($stmt->nextRowset());
    return $sets;
}

$setup = connect(true);
$setup->exec("DROP TABLE IF EXISTS cr_t");
$setup->exec("CREATE TABLE cr_t (id INT PRIMARY KEY, v INT)");
$setup->exec("INSERT INTO cr_t VALUES (1, 10), (2, 20)");
foreach (['cr_two', 'cr_arg'] as $p) {
    $setup->exec("DROP PROCEDURE IF EXISTS $p");
}
$setup->exec("CREATE PROCEDURE cr_two() BEGIN SELECT COUNT(*) AS n FROM cr_t; SELECT v FROM cr_t ORDER BY id; END");
$setup->exec("CREATE PROCEDURE cr_arg(IN p INT) BEGIN SELECT v FROM cr_t WHERE id = p; END");

foreach (['emulated' => true, 'native' => false] as $mode => $emulate) {
    try {
        $pdo = connect($emulate);

        $sets = rowsets($pdo->query("CALL cr_two()"));
        // With native prepares PDO reports the CALL's closing status as one more,
        // empty rowset; MySQL 8.4 gives the same. Count the SELECTs' sets.
        $selects = array_values(array_filter($sets, fn($set) => $set !== []));
        check("$mode: CALL returns two result sets", count($selects) === 2, json_encode($sets));
        check("$mode: first result set", ($sets[0][0]['n'] ?? null) == 2, json_encode($sets));
        check("$mode: second result set", array_column($sets[1] ?? [], 'v') == [10, 20], json_encode($sets));

        $stmt = $pdo->prepare("CALL cr_arg(?)");
        $stmt->execute([2]);
        $sets = rowsets($stmt);
        check("$mode: prepared CALL with an argument", ($sets[0][0]['v'] ?? null) == 20, json_encode($sets));

        $n = $pdo->query("SELECT COUNT(*) AS n FROM cr_t")->fetchColumn();
        check("$mode: connection usable after CALL", $n == 2, var_export($n, true));
    } catch (\Throwable $e) {
        check("$mode: CALL result sets", false, $e->getMessage());
    }
}

echo "\n$pass_n passed, $fail_n failed\n";
exit($fail_n > 0 ? 1 : 0);
