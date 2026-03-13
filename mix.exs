defmodule SelectoDBMariaDB.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/seeken/selecto_db_mariadb"

  def project do
    [
      app: :selecto_db_mariadb,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "SelectoDBMariaDB",
      description: "MariaDB adapter package for Selecto",
      source_url: @source_url,
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      adapter_contract_dep(),
      {:myxql, "~> 0.7.0"},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false}
    ]
  end

  defp adapter_contract_dep do
    if use_local_ecosystem?() do
      {:selecto_db_adapter, path: "../selecto_db_adapter"}
    else
      {:selecto_db_adapter, ">= 0.1.0 and < 0.2.0"}
    end
  end

  defp use_local_ecosystem? do
    case System.get_env("SELECTO_ECOSYSTEM_USE_LOCAL") do
      value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] -> true
      _ -> false
    end
  end

  defp package do
    [
      licenses: ["O-Saasy"],
      links: %{
        "GitHub" => @source_url,
        "Selecto" => "https://github.com/seeken/selecto",
        "SelectoDBAdapter" => "https://github.com/seeken/selecto_db_adapter"
      }
    ]
  end
end
