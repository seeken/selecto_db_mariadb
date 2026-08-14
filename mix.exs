defmodule SelectoDBMariaDB.MixProject do
  use Mix.Project

  @version "0.5.0"
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
      selecto_dep(),
      {:myxql, "~> 0.7.0"},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false}
    ]
  end

  defp selecto_dep do
    if use_local_ecosystem?() do
      {:selecto, path: local_selecto_path()}
    else
      {:selecto, ">= 0.5.0 and < 0.6.0"}
    end
  end

  defp local_selecto_path do
    "SELECTO_ECOSYSTEM_SELECTO_PATH"
    |> System.get_env("../selecto")
    |> Path.expand(__DIR__)
  end

  defp use_local_ecosystem? do
    case System.get_env("SELECTO_ECOSYSTEM_USE_LOCAL") do
      value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] -> true
      value when value in ["0", "false", "FALSE", "no", "NO", "off", "OFF"] -> false
      _ -> File.dir?(Path.expand("../selecto", __DIR__))
    end
  end

  defp package do
    [
      licenses: ["LicenseRef-O-Saasy"],
      links: %{
        "GitHub" => @source_url,
        "Selecto" => "https://github.com/seeken/selecto"
      }
    ]
  end
end
