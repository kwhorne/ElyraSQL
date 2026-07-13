<?php
/**
 * Laravel/Eloquent compatibility harness for ElyraSQL.
 *
 * Runs a realistic Eloquent workload (migrations, models, CRUD, relationships,
 * the query builder, transactions, delete/cascade) against a live ElyraSQL
 * server, using PDO + emulated prepared statements exactly as a stock Laravel
 * app would. Connection is configured from the environment so CI can inject the
 * host/port:
 *
 *   ELYRASQL_HOST (default 127.0.0.1)
 *   ELYRASQL_PORT (default 3307)
 *   ELYRASQL_DB   (default elyra)
 *   ELYRASQL_USER (default root)
 *   ELYRASQL_PASS (default '')
 *
 * Exits non-zero if any assertion fails.
 */
require __DIR__.'/vendor/autoload.php';

use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Eloquent\Model;

$capsule = new Capsule;
$capsule->addConnection([
    'driver'    => 'mysql',
    'host'      => getenv('ELYRASQL_HOST') ?: '127.0.0.1',
    'port'      => (int)(getenv('ELYRASQL_PORT') ?: 3307),
    'database'  => getenv('ELYRASQL_DB') ?: 'elyra',
    'username'  => getenv('ELYRASQL_USER') ?: 'root',
    'password'  => getenv('ELYRASQL_PASS') ?: '',
    'charset'   => 'utf8mb4',
    'collation' => 'utf8mb4_unicode_ci',
    'prefix'    => '',
    'options'   => [PDO::ATTR_EMULATE_PREPARES => true],
]);
$capsule->setAsGlobal();
$capsule->bootEloquent();

$schema = Capsule::schema();
$db = Capsule::connection();

$pass = 0; $fail = 0;
function check($name, $cond, $extra = '') {
    global $pass, $fail;
    if ($cond) { $pass++; echo "  ok   $name\n"; }
    else { $fail++; echo "  FAIL $name  $extra\n"; }
}
function section($s){ echo "\n== $s ==\n"; }

// ---------------- MIGRATIONS ----------------
section("Migrations (Schema builder)");
try {
    $schema->dropIfExists('posts');
    $schema->dropIfExists('users');
    $schema->create('users', function (Blueprint $t) {
        $t->id();
        $t->string('name');
        $t->string('email')->unique();
        $t->integer('age')->nullable();
        $t->decimal('balance', 10, 2)->default(0);
        $t->boolean('active')->default(true);
        $t->timestamps();
    });
    check("create users table", $schema->hasTable('users'));
    $schema->create('posts', function (Blueprint $t) {
        $t->id();
        $t->foreignId('user_id')->constrained()->onDelete('cascade');
        $t->string('title');
        $t->text('body')->nullable();
        $t->integer('views')->default(0);
        $t->timestamps();
    });
    check("create posts table (FK)", $schema->hasTable('posts'));
    check("hasColumn users.email", $schema->hasColumn('users', 'email'));
    $schema->table('posts', function (Blueprint $t) { $t->index('views'); });
    check("add index on posts.views", true);
} catch (\Throwable $e) { check("migrations", false, $e->getMessage()); }

// ---------------- MODELS ----------------
class User extends Model {
    public $timestamps = true;
    protected $guarded = [];
    public function posts() { return $this->hasMany(Post::class); }
}
class Post extends Model {
    public $timestamps = true;
    protected $guarded = [];
    public function user() { return $this->belongsTo(User::class); }
}

// ---------------- ELOQUENT CRUD ----------------
section("Eloquent CRUD");
try {
    $u = User::create(['name'=>'Alice','email'=>'alice@x.com','age'=>30,'balance'=>100.50]);
    check("create + lastInsertId", $u->id == 1, "id={$u->id}");
    $u2 = User::create(['name'=>'Bob','email'=>'bob@x.com','age'=>25]);
    check("second insert id=2", $u2->id == 2, "id={$u2->id}");
    $found = User::find(1);
    check("find(1)", $found && $found->name === 'Alice');
    check("nullable age on Bob", User::find(2)->age == 25);
    check("decimal default balance", (string)User::find(2)->balance === '0.00', "bal=".User::find(2)->balance);
    $found->update(['age'=>31,'balance'=>150.75]);
    check("update", User::find(1)->age == 31 && (string)User::find(1)->balance === '150.75');
    check("where + first", User::where('email','alice@x.com')->first()->id == 1);
    check("count", User::count() == 2);
    check("whereIn", User::whereIn('id',[1,2])->get()->count() == 2);
    check("orderBy + pluck", User::orderBy('age','desc')->pluck('name')->first() === 'Alice');
} catch (\Throwable $e) { check("crud", false, $e->getMessage()); }

// ---------------- RELATIONSHIPS ----------------
section("Relationships");
try {
    $alice = User::find(1);
    $alice->posts()->create(['title'=>'First','body'=>'hello','views'=>10]);
    $alice->posts()->create(['title'=>'Second','views'=>20]);
    check("hasMany create", $alice->posts()->count() == 2);
    check("belongsTo", Post::first()->user->name === 'Alice');
    $users = User::with('posts')->get();
    check("eager load with()", $users->first()->relationLoaded('posts'));
    check("withCount", User::withCount('posts')->find(1)->posts_count == 2);
    check("relation sum", (int)$alice->posts()->sum('views') == 30);
} catch (\Throwable $e) { check("relationships", false, $e->getMessage()); }

// ---------------- QUERY BUILDER ----------------
section("Query builder");
try {
    $rows = $db->table('posts')
        ->join('users','posts.user_id','=','users.id')
        ->select('users.name','posts.title')
        ->orderBy('posts.id')->get();
    check("join + select", $rows->count() == 2 && $rows[0]->name === 'Alice');
    $agg = $db->table('posts')->selectRaw('COUNT(*) c, SUM(views) s, AVG(views) a')->first();
    check("aggregates", $agg->c == 2 && $agg->s == 30);
    $grp = $db->table('posts')->groupBy('user_id')->havingRaw('COUNT(*) > 1')->select('user_id')->get();
    check("groupBy + having", $grp->count() == 1);
    check("exists()", $db->table('users')->where('id',1)->exists());
    check("paginate", User::paginate(1)->count() == 1);
    $db->table('users')->updateOrInsert(['email'=>'alice@x.com'], ['name'=>'Alice2']);
    check("updateOrInsert (existing)", User::where('email','alice@x.com')->first()->name === 'Alice2');
} catch (\Throwable $e) { check("query builder", false, $e->getMessage()); }

// ---------------- TRANSACTIONS ----------------
section("Transactions");
try {
    try {
        Capsule::connection()->transaction(function() {
            User::create(['name'=>'Temp','email'=>'temp@x.com']);
            throw new \Exception('rollback');
        });
    } catch (\Throwable $e) {}
    check("transaction rollback", User::where('email','temp@x.com')->count() == 0);
    Capsule::connection()->transaction(function() {
        User::create(['name'=>'Committed','email'=>'c@x.com']);
    });
    check("transaction commit", User::where('email','c@x.com')->count() == 1);
} catch (\Throwable $e) { check("transactions", false, $e->getMessage()); }

// ---------------- DELETE / CASCADE ----------------
section("Delete");
try {
    $bob = User::find(2);
    $bob->delete();
    check("model delete", User::find(2) === null);
    User::where('email','c@x.com')->delete();
    check("query delete", User::where('email','c@x.com')->count() == 0);
} catch (\Throwable $e) { check("delete", false, $e->getMessage()); }

echo "\n=========================\n";
echo "  $pass passed, $fail failed\n";
echo "=========================\n";
exit($fail > 0 ? 1 : 0);
