
Gem::Specification.new do |spec|
  spec.name          = "embulk-filter-csv_lookup"
  spec.version       = "0.1.0"
  spec.authors       = ["ankit-navral"]
  spec.summary       = %[Csv Lookup filter plugin for Embulk]
  spec.description   = %[Csv Lookup]
  spec.email         = ["ankit.navral@infoobjects.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/ankit.navral/embulk-filter-csv_lookup"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['~> 12.0']
end
