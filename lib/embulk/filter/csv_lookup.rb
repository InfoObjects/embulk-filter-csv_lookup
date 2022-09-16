Embulk::JavaPlugin.register_filter(
  "csv_lookup", "org.embulk.filter.csv_lookup.CsvLookupFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
