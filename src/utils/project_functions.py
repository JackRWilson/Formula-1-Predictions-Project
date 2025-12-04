# Jack Wilson
# 12/4/2025
# This file defines general project specific functions and variables

# ==============================================================================================
# I. Constructors Common Name Map
# ==============================================================================================

# Constructors common name mapping
constructor_mapping = {'team_id': {
    # Red Bull
    'Red Bull Racing Renault': 'Red Bull',
    'Red Bull Renault': 'Red Bull',
    'RBR Renault': 'Red Bull',
    'RBR Cosworth': 'Red Bull',
    'RBR Ferrari': 'Red Bull',
    'Red Bull Racing TAG Heuer': 'Red Bull',
    'Red Bull Racing Honda': 'Red Bull',
    'Red Bull Racing RBPT': 'Red Bull',
    'Red Bull Racing Honda RBPT': 'Red Bull',
    'Red Bull Racing': 'Red Bull',
    
    # AlphaTauri/Toro Rosso
    'Toro Rosso': 'Toro Rosso',
    'STR Ferrari': 'Toro Rosso',
    'STR Renault': 'Toro Rosso',
    'STR Cosworth': 'Toro Rosso',
    'Toro Rosso Ferrari': 'Toro Rosso',
    'Scuderia Toro Rosso Honda': 'Toro Rosso',
    'AlphaTauri Honda': 'AlphaTauri',
    'AlphaTauri RBPT': 'AlphaTauri',
    'AlphaTauri Honda RBPT': 'AlphaTauri',
    
    # Racing Bulls
    'RB Honda RBPT': 'Racing Bulls',
    
    # Ferrari
    'Ferrari': 'Ferrari',
    'Ferrari Jaguar': 'Ferrari',
    'Thin Wall Ferrari': 'Ferrari',
    
    # Mercedes
    'Mercedes': 'Mercedes',
    'Mercedes-Benz': 'Mercedes',
    
    # Aston Martin
    'Aston Martin Mercedes': 'Aston Martin',
    'Aston Martin Aramco Mercedes': 'Aston Martin',
    'Aston Butterworth': 'Aston Martin',
    'Aston Martin': 'Aston Martin',
    
    # McLaren
    'McLaren Ford': 'McLaren',
    'McLaren TAG': 'McLaren',
    'McLaren Honda': 'McLaren',
    'McLaren Peugeot': 'McLaren',
    'McLaren Renault': 'McLaren',
    'McLaren BRM': 'McLaren',
    'McLaren Mercedes': 'McLaren',
    'McLaren Serenissima': 'McLaren',
    'Mclaren BRM': 'McLaren',
    'McLaren Alfa Romeo': 'McLaren',
    
    # Williams
    'Williams Ford': 'Williams',
    'Williams Renault': 'Williams',
    'Williams Honda': 'Williams',
    'Williams Judd': 'Williams',
    'Williams BMW': 'Williams',
    'Williams Toyota': 'Williams',
    'Williams Cosworth': 'Williams',
    'Williams Mecachrome': 'Williams',
    'Williams Supertec': 'Williams',
    'Williams Mercedes': 'Williams',
    'Frank Williams Racing Cars/Williams': 'Williams',
    
    # Renault
    'Renault': 'Renault',

    # Alpine
    'Alpine Renault': 'Alpine',
    
    # Lotus
    'Lotus Renault': 'Lotus',
    'Lotus Ford': 'Lotus',
    'Lotus Climax': 'Lotus',
    'Lotus BRM': 'Lotus',
    'Lotus Honda': 'Lotus',
    'Lotus Judd': 'Lotus',
    'Lotus Lamborghini': 'Lotus',
    'Lotus Mugen Honda': 'Lotus',
    'Lotus Mercedes': 'Lotus',
    'Lotus Cosworth': 'Lotus',
    'Lotus Maserati': 'Lotus',
    'Lotus Pratt & Whitney': 'Lotus',
    
    # Force India
    'Force India Ferrari': 'Force India',
    'Force India Mercedes': 'Force India',

    # Racing Point
    'Racing Point BWT Mercedes': 'Racing Point',

    # Sauber
    'Sauber': 'Sauber',
    'Sauber Ferrari': 'Sauber',
    'Sauber Petronas': 'Sauber',
    'Sauber BMW': 'Sauber',
    'Sauber Mercedes': 'Sauber',
    'Sauber Ford': 'Sauber',
    'Kick Sauber Ferrari': 'Sauber',

    # Alfa Romeo
    'Alfa Romeo Racing Ferrari': 'Alfa Romeo',
    'Alfa Romeo Ferrari': 'Alfa Romeo',
    'Alfa Romeo': 'Alfa Romeo',
    
    # Haas
    'Haas Ferrari': 'Haas',
    'Haas F1 Team': 'Haas',
    
    # Jordan
    'Jordan Ford': 'Jordan',
    'Jordan Peugeot': 'Jordan',
    'Jordan Hart': 'Jordan',
    'Jordan Honda': 'Jordan',
    'Jordan Yamaha': 'Jordan',
    'Jordan Toyota': 'Jordan',
    'Jordan Mugen Honda': 'Jordan',
    
    # BAR
    'BAR Honda': 'BAR',
    'BAR Supertec': 'BAR',
    
    # Honda
    'Honda': 'Honda',
    
    # Benetton
    'Benetton Ford': 'Benetton',
    'Benetton BMW': 'Benetton',
    'Benetton Renault': 'Benetton',
    'Benetton Playlife': 'Benetton',
    
    # Toyota
    'Toyota': 'Toyota',
    
    # Jaguar
    'Jaguar Cosworth': 'Jaguar',
    
    # Stewart
    'Stewart Ford': 'Stewart',
    
    # BRM
    'BRM': 'BRM',
    'BRM Climax': 'BRM',

    # JBW
    'JBW Maserati': 'JBW',
    'JBW Climax': 'JBW',
    
    # Cooper
    'Cooper Climax': 'Cooper',
    'Cooper Maserati': 'Cooper',
    'Cooper Bristol': 'Cooper',
    'Cooper Castellotti': 'Cooper',
    'Cooper BRM': 'Cooper',
    'Cooper JAP': 'Cooper',
    'Cooper Alta': 'Cooper',
    'Cooper Borgward': 'Cooper',
    'Cooper Alfa Romeo': 'Cooper',
    'Cooper Ferrari': 'Cooper',
    'Cooper ATS': 'Cooper',
    'Cooper Ford': 'Cooper',
    'Cooper OSCA': 'Cooper',
    
    # Brabham
    'Brabham Climax': 'Brabham',
    'Brabham Repco': 'Brabham',
    'Brabham Ford': 'Brabham',
    'Brabham Alfa Romeo': 'Brabham',
    'Brabham BMW': 'Brabham',
    'Brabham BRM': 'Brabham',
    'Brabham Judd': 'Brabham',
    'Brabham Yamaha': 'Brabham',
    
    # Maserati
    'Maserati': 'Maserati',
    'Maserati Offenhauser': 'Maserati',
    'Maserati Milano': 'Maserati',
    'Maserati-Offenhauser': 'Maserati',
    'Maserati OSCA': 'Maserati',
    'Maserati Plate': 'Maserati',
    
    # Ligier
    'Ligier Matra': 'Ligier',
    'Ligier Ford': 'Ligier',
    'Ligier Renault': 'Ligier',
    'Ligier Megatron': 'Ligier',
    'Ligier Mugen Honda': 'Ligier',
    
    # Tyrrell
    'Tyrrell Ford': 'Tyrrell',
    'Tyrrell Renault': 'Tyrrell',
    'Tyrrell Honda': 'Tyrrell',
    'Tyrrell Yamaha': 'Tyrrell',
    'Tyrrell Ilmor': 'Tyrrell',
    
    # Arrows/Footwork
    'Arrows Ford': 'Arrows',
    'Arrows BMW': 'Arrows',
    'Arrows Megatron': 'Arrows',
    'Arrows Yamaha': 'Arrows',
    'Arrows Supertec': 'Arrows',
    'Arrows Asiatech': 'Arrows',
    'Arrows Cosworth': 'Arrows',
    'Arrows': 'Arrows',
    'Footwork Ford': 'Footwork',
    'Footwork Hart': 'Footwork',
    'Footwork Mugen Honda': 'Footwork',
    'Footwork Porsche': 'Footwork',
    
    # Vanwall
    'Vanwall': 'Vanwall',
    
    # Wolf
    'Wolf Ford': 'Wolf',
    'Wolf-Williams': 'Wolf',
    
    # Lola
    'Lola Ford': 'Lola',
    'Lola Lamborghini': 'Lola',
    'Lola Climax': 'Lola',
    'Lola BMW': 'Lola',
    'Lola Hart': 'Lola',
    'Lola Ferrari': 'Lola',

    # March
    'March Ford': 'March',
    'March Judd': 'March',
    'March Ilmor': 'March',
    'March Alfa Romeo': 'March',

    # Minardi
    'Minardi Ford': 'Minardi',
    'Minardi Ferrari': 'Minardi',
    'Minardi Lamborghini': 'Minardi',
    'Minardi Asiatech': 'Minardi',
    'Minardi Cosworth': 'Minardi',
    'Minardi Fondmetal': 'Minardi',
    'Minardi European': 'Minardi',
    'Minardi Hart': 'Minardi',
    'Minardi Motori Moderni': 'Minardi',
    
    # LDS
    'LDS Alfa Romeo': 'LDS',
    'LDS Climax': 'LDS',
    'LDS Repco': 'LDS',

    # Porche
    'Porsche (F2)': 'Porsche',
    'Porsche': 'Porsche',
    'Behra-Porsche': 'Porsche',

    # Scirocco
    'Scirocco BRM': 'Scirocco',
    'Scirocco Climax': 'Scirocco',

    # AFM
    'AFM Kuchen': 'AFM',
    'AFM BMW': 'AFM',
    'AFM Bristol': 'AFM',

    # ATS
    'ATS Ford': 'ATS',
    'ATS': 'ATS',
    'ATS BMW': 'ATS',
    'Derrington-Francis ATS': 'ATS',

    # Leyton House
    'Leyton House Judd': 'Leyton House',
    'Leyton House Ilmor': 'Leyton House',

    # Prost
    'Prost Mugen Honda': 'Prost',
    'Prost Peugeot': 'Prost',
    'Prost Acer': 'Prost',

    # Dallara
    'Dallara Judd': 'Dallara',
    'Dallara Ferrari': 'Dallara',
    'Dallara Ford': 'Dallara',

    # Larrousse
    'Larrousse Lamborghini': 'Larrousse',
    'Larrousse Ford': 'Larrousse',

    # Osella
    'Osella Ford': 'Osella',
    'Osella Alfa Romeo': 'Osella',
    'Osella': 'Osella',
    'Osella Hart': 'Osella',

    # Kurtis Kraft
    'Kurtis Kraft Offenhauser': 'Kurtis Kraft',
    'Kurtis Kraft Novi': 'Kurtis Kraft',
    'Kurtis Kraft Cummins': 'Kurtis Kraft',

    # Marussia
    'Marussia Cosworth': 'Marussia',
    'Marussia Ferrari': 'Marussia',

    # Gordini
    'Simca-Gordini': 'Gordini',
    'Gordini': 'Gordini',

    # Connaught
    'Connaught Lea Francis': 'Connaught',
    'Connaught Alta': 'Connaught',

    # Eagle
    'Eagle Climax': 'Eagle',
    'Eagle Weslake': 'Eagle',

    # RAM
    'RAM Ford': 'RAM',
    'RAM Hart': 'RAM',

    # Shadow
    'Shadow Ford': 'Shadow',
    'Shadow Matra': 'Shadow',

    # Matra
    'Matra Ford': 'Matra',
    'Matra': 'Matra',
    'Matra Cosworth': 'Matra',
    'Matra BRM': 'Matra',

    # ERA
    'ERA': 'ERA',
    'ERA Bristol': 'ERA',

    # Spirit
    'Spirit Honda': 'Spirit',   
    'Spirit Hart': 'Spirit',

    # Frazer Nash
    'Frazer Nash': 'Frazer Nash',
    'Frazer Nash Bristol': 'Frazer Nash',

    # Emeryson
    'Emeryson Alta': 'Emeryson',
    'Emeryson Climax': 'Emeryson',

    # De Tomaso
    'De Tomaso OSCA': 'De Tomaso',
    'De Tomaso Alfa Romeo': 'De Tomaso',
    'De Tomaso Ford': 'De Tomaso',

    # Gilby
    'Gilby Climax': 'Gilby',
    'Gilby BRM': 'Gilby',

    # Tecno
    'Tecno': 'Tecno',
    'Tecno Cosworth': 'Tecno',

    # Ligier
    'Ligier Judd': 'Ligier',
    'Ligier Lamborghini': 'Ligier',

    # Euro Brun
    'Euro Brun Judd': 'Euro Brun',
    'Euro Brun Ford': 'Euro Brun',


    # Other
    'No Team': 'Privateer',
    'Toleman Hart': 'Toleman',       
    'Venturi Lamborghini': 'Venturi',        
    'Onyx Ford': 'Onyx',
    'AGS Ford': 'AGS',   
    'Rial Ford': 'Rial',
    'Zakspeed': 'Zakspeed',
    'Theodore Ford': 'Theodore',
    'Deidt Offenhauser': 'Deidt',
    'Sherman Offenhauser': 'Sherman',
    'Schroeder Offenhauser': 'Schroeder',
    'Kuzma Offenhauser': 'Kuzma',
    'Lesovsky Offenhauser': 'Lesovsky',
    'Watson Offenhauser': 'Watson',
    'Phillips Offenhauser': 'Phillips',
    'Epperly Offenhauser': 'Epperly',
    'Trevis Offenhauser': 'Trevis',
    'HRT Cosworth': 'HRT',
    'Virgin Cosworth': 'Virgin',
    'Caterham Renault': 'Caterham',
    'Milano Speluzzi': 'Milano',
    'Turner Offenhauser': 'Turner',
    'Alta': 'Alta',    
    'Moore Offenhauser': 'Moore',
    'Nichels Offenhauser': 'Nichels',
    'Marchese Offenhauser': 'Marchese',
    'Stevens Offenhauser': 'Stevens',
    'Langley Offenhauser': 'Langley',
    'Ewing Offenhauser': 'Ewing',   
    'Rae Offenhauser': 'Rae',
    'Olson Offenhauser': 'Olson',
    'Wetteroth Offerhauser': 'Wetteroth',
    'Snowberger Offenhauser': 'Snowberger',
    'Adams Offenhauser': 'Adams',
    'HWM Alta': 'HWM',    
    'Lancia': 'Lancia',
    'Talbot-Lago': 'Talbot-Lago',
    'BRP BRM': 'BRP',
    'Hesketh Ford': 'Hesketh',
    'Hill Ford': 'Hill',
    'Ensign Ford': 'Ensign',
    'Penske Ford': 'Penske',
    'Fittipaldi Ford': 'Fittipaldi',
    'ISO Marlboro Ford': 'ISO Marlboro',
    'Iso Marlboro Ford': 'ISO Marlboro',
    'Surtees Ford': 'Surtees',
    'Parnelli Ford': 'Parnelli',
    'Super Aguri Honda': 'Super Aguri',
    'MRT Mercedes': 'Manor',
    'Brawn Mercedes': 'Brawn',
    'Spyker Ferrari': 'Spyker',
    'MF1 Toyota': 'Midland',
    'Veritas': 'Veritas',
    'Pawl Offenhauser': 'Pawl',
    'Hall Offenhauser': 'Hall',
    'Bromme Offenhauser': 'Bromme',
    'OSCA': 'OSCA',
    'BMW': 'BMW',
    'EMW': 'EMW',
    'Pankratz Offenhauser': 'Pankratz',
    'Bugatti': 'Bugatti',
    'Klenk BMW': 'Klenk',
    'Dunn Offenhauser': 'Dunn',    
    'Elder Offenhauser': 'Elder',
    'Christensen Offenhauser': 'Christensen',
    'Sutton Offenhauser': 'Sutton',
    'Tec-Mec Maserati': 'Tec-Mec',
    'Meskowski Offenhauser': 'Meskowski',
    'Scarab': 'Scarab',
    'Ferguson Climax': 'Ferguson',
    'ENB Maserati': 'ENB',
    'Stebro Ford': 'Stebro',               
    'Shannon Climax': 'Shannon',     
    'Protos Cosworth': 'Protos',   
    'Bellasi Ford': 'Bellasi',       
    'Eifelland Ford': 'Eifelland',
    'Politoys Ford': 'Politoys',
    'Connew Ford': 'Connew',
    'Trojan Ford': 'Trojan',
    'Amon Ford': 'Amon',
    'Token Ford': 'Token',
    'Lyncar Ford': 'Lyncar',
    'Boro Ford': 'Boro',
    'Kojima Ford': 'Kojima',
    'LEC Ford': 'LEC',
    'Merzario Ford': 'Merzario',
    'Martini Ford': 'Martini',
    'Rebaque Ford': 'Rebaque',
    'AGS Motori Moderni': 'AGS',
    'Coloni Ford': 'Coloni',
    'Zakspeed Yamaha': 'Zakspeed',
    'Fondmetal Ford': 'Fondmetal',
    'Moda Judd': 'Moda',    
    'Simtek Ford': 'Simtek',
    'Pacific Ilmor': 'Pacific',
    'Forti Ford': 'Forti',
    'Lambo Lamborghini': 'Modena'
}}