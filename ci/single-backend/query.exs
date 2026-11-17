backend=List.first(System.argv())
if backend in ["core","scaffold"] do
  true=Code.ensure_loaded?(Selecto)
  IO.puts(if backend=="core", do: "CORE_IMPORT_OK", else: "SCAFFOLD_IMPORT_OK (no extension implementation/query claimed)")
else
  adapter=case backend do
    "mysql" -> Selecto.DB.MySQL
    "mariadb" -> SelectoDBMariaDB.Adapter
    "sqlite" -> SelectoDBSQLite.Adapter
    "duckdb" -> SelectoDBDuckDB.Adapter
  end
  opts=if backend in ["mysql","mariadb"], do: [hostname: System.fetch_env!("DB_HOST"),port: String.to_integer(System.fetch_env!("DB_PORT")),username: "root",database: "single_backend"], else: [database: ":memory:"]
  {:ok,c}=adapter.connect(opts)
  {:ok,_}=adapter.execute(c,"CREATE TABLE smoke_items(id integer PRIMARY KEY,name varchar(40))",[],[])
  {:ok,_}=adapter.execute(c,"INSERT INTO smoke_items VALUES(1,'selected')",[],[])
  d=%{name: "Smoke",source: %{source_table: "smoke_items",primary_key: :id,fields: [:id,:name],columns: %{id: %{type: :integer},name: %{type: :string}},associations: %{}},schemas: %{},joins: %{}}
  q=d |> Selecto.configure(c,adapter: adapter,validate: false) |> Selecto.select(["id","name"])
  {:ok,{[[1,"selected"]],_,_}}=Selecto.execute(q)
  IO.puts("SELECTED_QUERY_OK #{backend}")
  adapter.disconnect(c)
end
