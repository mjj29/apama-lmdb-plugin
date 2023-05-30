#include<epl_plugin.hpp>
#include<lmdb.h>

using namespace com::apama::epl;

class LMDBPlugin: public EPLPlugin<LMDBPlugin>
{
	static void executefn( auto fn , auto &&...  args)
	{
		if (int err = fn(args...); err)
		{ throw std::runtime_error(mdb_strerror(err)); }
	}
	struct MDBEnv
	{
		MDBEnv(const MDBEnv &) = delete;
		MDBEnv(MDBEnv &&) = delete;
		MDBEnv &operator=(const MDBEnv &) = delete;
		MDBEnv &operator=(MDBEnv &&) = delete;
		MDBEnv() {
			executefn(mdb_env_create, &env);
		}
		~MDBEnv() { if (env) { mdb_env_close(env); } }
		operator MDB_env *() { return env; }
		MDB_env &operator *() { return *env; }
		MDB_env *operator ->() { return env; }
		MDB_env *env=nullptr;
	};
	template<typename Class, auto MemberFunc, typename... Args>
	auto chunkfn(const custom_t<Class> &chunk, Args&&... args) {
		return ((*chunk).*MemberFunc)(env, std::forward<Args&&>(args)...);
	}
	struct Database
	{
		Database(MDBEnv &env, const char *name, MDB_txn *txn): env(env)
		{
			executefn(mdb_dbi_open, txn, name, 0, &db);
		}
		~Database()
		{
		}
		MDBEnv &env;
		MDB_dbi db;
	};
	using DBChunk = custom_t<Database>;
	struct Transaction
	{
		DBChunk opendb(MDBEnv &env, const char *name)
		{
			return DBChunk(new Database(env, name, txn));
		}
		MDB_txn *txn;
	};
	using TxnChunk = custom_t<Transaction>;
public:
	LMDBPlugin(): base_plugin_t("LMDBPlugin")
	{
		logger.info("Creating LMDB plugin using LMDB version %s\n", mdb_version(nullptr, nullptr, nullptr));
	}
	static void initialize(base_plugin_t::method_data_t &md)
	{
		md.registerMethod<decltype(&LMDBPlugin::openenv), &LMDBPlugin::openenv>("openenv");
		md.registerMethod<decltype(&LMDBPlugin::chunkfn<Transaction,&Transaction::opendb,char const * const &>), &LMDBPlugin::chunkfn<Transaction,&Transaction::opendb,char const * const &>>("transaction_opendb");
	}
	void openenv(const char *path)
	{
		logger.info("Opening LMDB database in path %s\n", path);
		executefn(mdb_env_open, env, path, MDB_NOLOCK|MDB_NOSYNC, 640);
	}
	MDBEnv env;
};

APAMA_DECLARE_EPL_PLUGIN(LMDBPlugin)
