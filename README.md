<p align="center">
  <a href="https://www.infoobjects.com/" target="blank"><img src="screenshots/logo.png" width="150" alt="InfoObjects Logo" /></a>
</p>
<p align="center">Infoobjects is a consulting company that helps enterprises transform how and where they run applications and infrastructure.
From strategy, to implementation, to ongoing managed services, Infoobjects creates tailored cloud solutions for enterprises at all stages of the cloud journey.</p>


# CSV lookup filter plugin for Embulk
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An Embulk filter plugin for Lookup Transformation with CSV

## Configuration

- **csv_lookup**: Required attributes for the LookUp Filter Plugin -
    - **filters**:
        - **type**: Name of lookup type (required)
    - **mapping_from**: (Name of columns to be matched with table 2 columns) (required)
        - **Name of column-1**: column name-1 from input file
        - **Name of column-2**: column name-2 from input file etc ...
    - **mapping_to**:   (Name of columns to be matched with table 1 columns) (required)
        - **Name of column-1**: column name-1 from input file
        - **Name of column-2**: column name-2 from input file
    - **new_columns**:   (New generated column names) (required)
        - **Name-1,Type-1**: Any Name, Type of the name { name: car_id, type: long }
        - **Name-2,Type-2**: Any Name, Type of the name { name: category, type: string }
        - **Name-3,Type-3**: Any Name, Type of the name { name: fuel_capacity, type: string }
    - **path_of_lookup_file**: lookup file path (required)
## Example - columns

Customer.csv for table 1 is as follows :-

```
id  customer_name       address                     email                       car_name    company
1   John Doe            123 Main St, Anytown USA    john.doe@example.com        Civic       Honda
2   Jane Smith          456 Elm St, Anytown USA     jane.smith@example.com      E-Class     Mercedes-Benz
3   Bob Johnson         789 Oak St, Anytown USA     bob.johnson@example.com     GLE-Class   Mercedes-Benz
4   Amanda Hernandez    999 Cedar St, Anytown USA   amanda.hernandez@example.com 911        119
5   Tom Brown           567 Pine St, Anytown USA    tom.brown@example.com       C-Class     Mercedes-Benz
6   Samantha Davis      890 Cedar St, Anytown USA   samantha.davis@example.com  Civic       Honda
7   Mike Wilson         1234 Spruce St, Anytown USA mike.wilson@example.com     GLE-Class   Mercedes-Benz
8   Jason Brown         888 Pine St, Anytown USA    jason.brown@example.com     911         Porsche
9   David Rodriguez     9010 Oak St, Anytown USA    david.rodriguez@example.com GLC-Class   Mercedes-Benz
10  Mark Davis          666 Spruce St, Anytown USA  mark.davis@example.com      C-Class     Mercedes-Benz
11  Chris Thompson      222 Cedar St, Anytown USA   chris.thompson@example.com  Cayenne     Porsche
12  Linda Young         555 Birch St, Anytown USA   linda.young@example.com     RAV4
13  Kevin Hernandez     444 Maple St, Anytown USA   kevin.hernandez@example.com 911         119
```

Car.csv for table 2 is as follows :-

```
car_id  model       brand            category   fuel_capacity  
87      GLE-Class   Mercedes-Benz   SUV         80
101     Cayenne     Porsche         SUV         75
119     911         Porsche         Sports Car  64
205     Accord      Honda           Sedan       56
334     Pilot       Honda           SUV         70
434     CR-v        Honda           SUV         64      
559     C-Class     Mercedes-Benz   Sedan       66
603     Civic       Honda           Sedan       42
697     E-Class     Mercedes-Benz   Sedan       72
812     GLC-Class   Mercedes-Benz   Sedan       68                            
    
```

As shown in yaml below, columns mentioned in mapping_from will be mapped with columns mentioned in mapping_to      
ie:
car_name : model                       
company : brand

After successful mapping an Output.csv file containing the columns mentioned in new_columns will be generated

Output File generated :-

```
id  customer_name       address                     email                       car_name    company         car_id  category   fuel_capacity  
1   John Doe            123 Main St, Anytown USA    john.doe@example.com        Civic       Honda           603     Sedan       42
2   Jane Smith          456 Elm St, Anytown USA     jane.smith@example.com      E-Class     Mercedes-Benz   697     Sedan       72 
3   Bob Johnson         789 Oak St, Anytown USA     bob.johnson@example.com     GLE-Class   Mercedes-Benz   87      SUV         80
4   Amanda Hernandez    999 Cedar St, Anytown USA   amanda.hernandez@example.com 911        119              0         
5   Tom Brown           567 Pine St, Anytown USA    tom.brown@example.com       C-Class     Mercedes-Benz   559     Sedan       66   
6   Samantha Davis      890 Cedar St, Anytown USA   samantha.davis@example.com  Civic       Honda           603     Sedan       42   
7   Mike Wilson         1234 Spruce St, Anytown USA mike.wilson@example.com     GLE-Class   Mercedes-Benz   87      SUV         80   
8   Jason Brown         888 Pine St, Anytown USA    jason.brown@example.com     911         Porsche         119     Sport Car   64   
9   David Rodriguez     9010 Oak St, Anytown USA    david.rodriguez@example.com GLC-Class   Mercedes-Benz   812     SUV         68
10  Mark Davis          666 Spruce St, Anytown USA  mark.davis@example.com      C-Class     Mercedes-Benz   559     Sedan       66   
11  Chris Thompson      222 Cedar St, Anytown USA   chris.thompson@example.com  Cayenne     Porsche         101     SUV         75   
12  Linda Young         555 Birch St, Anytown USA   linda.young@example.com     RAV4        \N               0  
13  Kevin Hernandez     444 Maple St, Anytown USA   kevin.hernandez@example.com 911         119              0                                                         
```

```yaml
filters:
  - type: csv_lookup
    mapping_from:
      - car_name
      - company
    mapping_to:
      - model
      - brand
    new_columns:
      - { name: car_id, type: long }
      - { name: category, type: string }
      - { name: fuel_capacity, type: string }
    path_of_lookup_file: "..path../car.csv"
```

Notes:
1. mapping_from attribute should be in the same the order as mentioned in the input file. 
2. In case with JDBC plugin if any integer column returned as float/decimal then use to cast that column as long as below
``` 
     column_options: 
     id: {value_type: long}
```
3. Matching columns data types must be int,long and String

## Development

Run example:

```
$ ./gradlew package
$ embulk run -I ./lib seed.yml
```

Deployment Steps:

```
Install ruby in your machine
$ gem install gemcutter (For windows OS)

$ ./gradlew gemPush
$ gem build NameOfYourPlugins (example: embulk-filter-csv_lookup)
$ gem push embulk-filter-csv_lookup-0.1.0.gem (You will get this name after running above command)
```

Release gem:

```
$ ./gradlew gemPush
```

## Licensing

InfoObjects [license](LICENSE) (MIT License)
