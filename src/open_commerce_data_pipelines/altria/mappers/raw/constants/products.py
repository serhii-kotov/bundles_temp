FLAVORS = ["menthol",
           "blueberry",
           "pineapple",
           "kiwi",
           "berry",
           "vanilla",
           "melon",
           "watermelon",
           "strawberry",
           "cognac",
           "honey",
           "bourbon",
           "grape",
           "rum",
           "tequila",
           "apple",
           "mango",
           "cherry",
           "peach",
           "coconut",
           "creamy",
           "french",
           "silky",
           "caribe",
           "calsico",
           "crema",
           "dolce,",
           "fresco",
           "noche",
           "tropical",
           "vista",
           "golden",
           "natural",
           "wild",
           "wine",
           "cranberry",
           "wintergreen",
           "peppermint",
           "citrus",
           "spearmint",
           "mint",
           "original",
           "brandy",
           "coffee",
           "cinnamon",
           "fresh",
           "harvest",
           "fruit",
           "dragon",
           "crema",
           "burst",
           "blend",
           "summer",
           "pumpkin",
           "spice",
           "muffin",
           "blackberry",
           "raspberry",
           "banana",
           "caramel",
           "chocolate",
           "guava",
           "lychee",
           "lemon",
           "pudding",
           "pie",
           "custard",
           "iced",
           "ice",
           "lemonade",
           "pinacolada",
           "funnel",
           "cake",
           "passion",
           "pomegranate",
           "bubble",
           "gum",
           "passionfruit",
           "honeydew",
           "macchiato",
           "danish",
           "cranapple",
           "maple",
           "donut",
           "grapefruit",
           "lime",
           "cantaloupe",
           "huckleberry",
           "kiwifruit",
           "marmalade",
           "nectarine",
           "apricot",
           "fig",
           "marionberry",
           "papaya",
           "boysenberry",
           "gooseberry"
           ]

FILTER_TYPES = ["non-filter", "filter"]

MANUFACTURER_BRAND_MAP = {
    "22nd Century Group": ["VLN"],
    "Alfred Dunhill Ltd.": ["Dunhill"],
    "Alternative Brands, Inc.": ["Tracker", "Tucson"],
    "American Cigarette Co": ["Union"],
    "Belcorp of America Inc": ["First One", "Galaxy Pro", "Rich", "Trust"],
    "Carolina Tobacco Co.": ["Roger"],
    "Cartier International BV": ["Cartier"],
    "Chancellor": ["Treasurer Luxury"],
    "Cherokee Tobacco Co.": ["Cherokee"],
    "Cheyenne International LLC": ["Aura", "Cheyenne", "Decade", "Derringer"],
    "CigTec Tobacco LLC": ["CT"],
    "Compania Tabacalera Internacional S.A. Paraguay": ["Director"],
    "Dhanraj International, Inc.": ["Dhyan", "Zanzibar"],
    "Dosal Tobacco Company": ["305", "Competidora", "DTC"],
    "Farmers Tobacco Co. Of Cynthiana, Inc.": ["Baron", "CTC", "Kentuckys Best", "VB Victory Brand"],
    "Firebird Manufacturing LLC.": ["Palmetto"],
    "G.A. Keranis S.A.": ["Winner"],
    "General Tobacco": ["Champion"],
    "Giel York Tobacco": ["Bronco"],
    "Global Tobacco Corporation": ["Classic GLBL", "Poker", "Clipper"],
    "Goodrich Tobacco Co": ["Red Sun"],
    "Grand River Enterprises": ["Couture", "Opal", "Seneca", ],
    "Grand Tobacco Co.": ["Garni"],
    "Great Swamp Enterprises Inc": ["Cayuga"],
    "GTC Industries Limited": ["Double Diamond"],
    "Hanyung (Hong Kong)": ["Double Happiness"],
    "Havana Tobacco Exporters Inc.": ["Havana"],
    "Heritage Tobacco Group Llc": ["Hobby"],
    "Imperial Tobacco Co.": ["Du Maurier"],
    "International Masis Tobacco Llc": ["Cobra"],
    "ITG Brands": ["Crowns", "Davidoff", "Fortuna", "Gauloises", "Kool", "Malibu", "Maverick", "Montclair", "Rave",
                   "Riviera", "Salem", "Sonoma", "Tuscany Dsct", "USA", "West", "Winston"],
    "ITW Manufacturing Ltd.": ["Marathon"],
    "JM  Ridgeworth Corp": ["Stallion"],
    "JTI-USA": ["Export A", "LD", "Wave", "Wings"],
    "K&K Distributors": ["Champion"],
    "King Maker Marketing Inc": ["Ace", "Checkers", "Gold Crest", "Hi-Val"],
    "King Mountain Tobacco Co.": ["King Mountain"],
    "Kretek International Inc": ["Dreams", "Liquid Zoo", "Tajmahal", "Borkum Riff", "NY Min", "EZ Cig", "Pixl",
                                 "Voodoo", "Ki"],
    "KT&G Corporation": ["Carnival", "This", "Timeless Time"],
    "Limitless Mod Co": ["Limitless"],
    "Lit": ["Lit"],
    "Lokee": ["Lokee"],
    "Lookah": ["Lookah"],
    "Liggett Group": ["Bronson LIG", "Class A", "Eve", "Grand Prix", "Jade", "Liggett Select", "Montego", "Pyramid",
                      "Tourney"],
    "Lignum-2 Inc.": ["Sampoerna"],
    "Monte Paz S.A.": ["Ocean"],
    "Native Trading Associates": ["Native"],
    "North Atlantic Cigarette Company Inc.": ["Zig-Zag"],
    "Ohserase Manufacturing": ["Great Country", "Signal", "Locos"],
    "P.T. Djarum": ["Djarum", "Djarum"],
    "Pacific Stanford Manufacturing Corporation": ["North"],
    "Patriot Tobacco Co.": ["Patriot", "Silver Cloud", "Real Precision"],
    "PM USA": ["Alpine", "B&H", "Basic", "Black & Gold", "Bucks", "Cambridge", "Chesterfield", "Cigarettello",
               "Classic NATS", "Clove", "Commander", "Daves", "English Ovals", "Fantasia", "Genco", "Havana Ovals",
               "L&M", "Lark", "Marlboro", "MCD", "Merit", "Multifilter", "Nat Sherman", "Nats", "New York Cut",
               "Parliament", "Players", "Rothmans", "Saratoga", "V Slims", "Va Circles"],
    "Premier Market": ["1839", "First Class", "Ice King", "Manitou", "Shield", "Ultra Buy", "Wild Horse"],
    "President Tobacco LLC": ["President"],
    "Q International": ["Westport"],
    "R.J. Reynolds Tobacco": ["Bee King", "Camel", "Century", "Doral", "Dunhill", "Eclipse", "Gold Coast", "GPC",
                              "Kamel", "Kent", "Lucky Strike", "Misty", "Marker RJR", "Monarch", "More", "Newport",
                              "Now", "Old Gold", "Pall Mall", "Planet", "Revo", "St Exp", "Sterling", "Tareyton",
                              "True", "Vantage", "Revel"],
    "RG Logistics INC": ["Future"],
    "Rothmans International": ["Craven"],
    "S&M Brands Inc": ["Baileys", "Riverside", "Tahoe", "Lex12"],
    "Sandia Tobacco": ["Royal", "Sandia"],
    "Santa Fe Natural Tobacco Company": ["Dunhil", "Nat Amer Spirit", "State Express 555"],
    "Seita": ["Gauloises", "Gitanes"],
    "Seminole Brands LLC": ["Seminole"],
    "Seneca Manufacturing Co.": ["Heron", "Sands"],
    "Seneca-Cayuga Tobacco Co.": ["Golden", "Seneca-Cayuga", "Skydance"],
    "Shanghai Tobacco Corp.": ["Chunghwa", "Golden Deer"],
    "Six Nations Manufacturing": ["Gator", "Native Pride", "Senate"],
    "Skookum Creek Tobacco Co.": ["Complete", "Premis", "Traditions"],
    "Smokin Joes Indian Trade": ["Buffalo", "Market", "Smokin Joes"],
    "Soex India Pvt. Ltd.": ["Soex"],
    "Sovereign Tobacco Co.": ["Bishop", "Cool Harbor", "Great Country", "Niagaras"],
    "Star Scientific": ["NY NY", "Vegas", "Ariva", "Stonewall"],
    "Tabacalera Del Este S.A.": ["Palmero"],
    "Tabak Haus-Von Eicken Group": ["Manitou Virginia"],
    "Tantus Tobacco LLC": ["Golden Blend", "24/7"],
    "Third Millenium Cigars & Assoc., Inc.": ["Golden Beach"],
    "Tobacco Holdings": ["Bridgeport"],
    "US Flue-Cured Tobacco Growers Inc.": ["1839", "Traffic"],
    "Vector Tobacco (USA) Ltd": ["Eagle 20s", "Quest", "Silver Eagle", "USA VT"],
    "Veneto Tabacos": ["American Hero"],
    "Virginia-Carolina Corp.": ["Boss", "Diva"],
    "Wind River Tobacco Co LLC": ["American Bison", "Nashville", "Teton"],
    "Xcaliber International": ["24/7", "Berkley", "Berley", "Echo", "Edgefield", "Exeter", "Golden Blend",
                               "Gunsmoke TNT Man", "Gunsmoke TNT Woman", "Mainstreet TNT", "Sport TNT", "Red Buck",
                               "Richwood"],

    "ITG Cigars Inc": ["1886", "AYC", "Backwoods", "Blk/Blue", "Dtch Mstr", "El Producto", "Factory Spcl", "Havana",
                       "HavATampa Jwl", "Jenuwine", "La Crona", "Miami Suites", "Muriel", "Phillies", "Roi Tan",
                       "Rustlers", "Supre", "Tampa Nuggt", "Wht Cat"],
    "Grand River Enterprises Six Nations Ltd": ["38 Special", "Chiefwoods", "Jukasa"],
    "Good Times USA, LLC": ["4 KS", "Cgr Ville", "City Life", "Dbl Maestro", "Exec Brnch", "FSS",
                            "Good Days Factory Seconds", "Good Times", "Happy Times", "HD", "Lil N Wld",
                            "Stonewood", "Swt Woods", "Wolf Bros"],
    "Dos Maures": ["3D Quatro"],
    "High Grade Herbal Inc.": ["50s Kng", "Nat Lf"],
    "Swisher Swts Cgr Co": ["Acid", "Blkstone", "Goodies", "Home Made", "Java", "Keep Moving", "Kng Edward", "Miramar",
                            "Optimo", "Pom Pom Operas", "Swisher Swts"],
    "Cuban Desire Corp": ["Adam/Eve"],
    "Agio Cigar Company": ["Agio Meharis Ecuador", "Panter Dominca", "Panther"],
    "Intercontinental Cigar": ["Al Capone"],
    "Arango Cigar Co.": ["Arango Sportsman"],
    "Ashton Distributors, Inc.": ["Ashton"],
    "Avanti Cigar Co": ["Avanti", "De Nobili", "Parodi", "Petri"],
    "New Image Global Inc": ["Ballers Choice", "Da Islnd Chce", "Players Chce", "Royl Blunts", "Smkr Friendly",
                             "Royl Blunts"],
    "National Cigar Corp": ["Ben Bay Crystl", "Charles Denby Invincible", "Dbl Pltnm", "El Verso", "Evermore", "Ibold",
                            "Marsh Wheeling", "Miflins Chce", "RG Dun"],
    "JMC": ["Blk&Mld", "Casa Del Sol", "Gld/Mld", "Middleton", "Prince Albert", "Royl Cmfrt"],
    "Prime Time Intl Co": ["Blunt Mstr", "Prime Tim"],
    "National Honey Almond, Inc.": ["Bluntville", "Cosa Nostra", "Dville", "Entourage", "Trivo"],
    "Bogey Cigars, LLC": ["Bogey", "Bogey Mstr", "Bogey Tree", "Bogey Woods"],
    "Scandinavian Tobacco": ["CAO", "Capt Blk", "Cubero", "H Wntrmns", "Havana", "Imp", "Imperial"],
    "Rocky Patel": ["Chief"],
    "National Tobacco Company": ["Cloud9", "Hi Fi", "Hype", "Zig Zag", "Smoker Friendly", "Stokers", "Ctn Mth",
                                 "Durango", "Riptide", "Tango", "V2", "Zig Zag"],
    "G Dannemann": ["Dannemann"],
    "Davidoff of Geneva": ["Davidoff"],
    "A&T Tobacco Imports Inc": ["Dbl Dmnd", "Hats Off", "Double Diamond"],
    "Dark Leaf Cigars Llcs": ["Dk Leaf Woods"],
    "Cdj Marketing Inc": ["Doopy Woods"],
    "J C Newman": ["Factory Thrwout", "Rigoletto", "Trdr Jacks"],
    "Global Tobacco LLC": ["Fiesta Spiral", "Suprm Blnd", "Pg 18", "X2O", "X2o", "Xeon"],
    "Frontier Brands LLC": ["Frontier"],
    "Fronto King Llc": ["Fronto Kng"],
    "GG Natural Leaf": ["GG"],
    "Grabba Leaf": ["Grabba Leaf"],
    "Swedish Match": ["GYV", "Jackpot", "Night Owl", "Robert Burns", "Tijuana", "Tiparillo", "Wht Owl", "General",
                      "Longhorn", "Redman", "Renegades", "Sequoia", "Thunder Xtrm", "Timber Wolf", "Zyn"],
    "Trendsettah USA Inc.": ["Hood Wrap", "On Woods", "Splitarillos"],
    "Hqc": ["Hqc"],
    "House of Windsor": ["Hse of Windsor", "Palma", "Wolf Bros HOW"],
    "Hunid Racks LLC": ["Hunid Racks Rasta"],
    "Globrands LLC": ["Hustler Woods", "Hustler"],
    "HBI Tobacco": ["Juicy Bluntarillo", "Zen"],
    "King Woods": ["Kng Woods"],
    "Grind Distribution": ["Kush"],
    "House Of Lucky Cigar": ["Lucky Flvr"],
    "USA Millennium LP": ["Millenium", "Royal Smoke", "Royl Smk"],
    "F.D. Grave and Son Inc": ["Muniemaker"],
    "P&L Sales Group Inc": ["Nomad Nat"],
    "OG Woods": ["OG Woods"],
    "Inter Continental Trading USA Inc": ["Ohm", "Vio"],
    "Oscar Valladares": ["Oscar Valladares"],
    "A&A Trading LLC": ["Rico Frsh"],
    "Vision Cigar & Cigarette LLC": ["Rock N Rl"],
    "Rb Tobacco Enterprises Llc": ["Rolling Blunts"],
    "Marsh Wheeling": ["Rough Cut"],
    "Romar Corp": ["Roxwell"],
    "Finck Cigar Co.": ["Sam Houston"],
    "Exoticos": ["Scarface"],
    "Show Cigars": ["Show", "Zpalma"],
    "USA Tobacco Co": ["Smkr Best"],
    "Primte Time Intl Co": ["Smkr Chce"],
    "Starbuzz Tobacco Inc": ["Starbuzz Buzzarillos"],
    "Swisher International": ["Swisher Swts", "Eagle", "Swisher"],
    "Miami Cigar Co": ["Tatiana Groovy"],
    "Ayade Cigars": ["Throwback"],
    "Manifatture Sigaro Toscano": ["Toscano"],
    "Finck Cigar": ["Travis Clb Clasc"],
    "Fx Smiths Sons": ["Tuscarora"],
    "Ultra Cigarillos": ["Ult"],
    "Villiger Cigars of North America": ["Villiger"],
    "Sunshine Tobacco": ["Ziparillos", "Etron"],
    "American Smokeless Tobacco": ["Dscret PP"],
    "American Snuff Co": ["Camel Dip", "Cougar Dsct Gen 2", "Grizzly", "Hawken", "Kodiak"],
    "Dynamis Ventures Inc": ["Silverback"],
    "Fat Lip Brands": ["Cooper", "Creek", "Kayak", "Redwood", "Silverado", "Star Value", "Swisher"],
    "Lake Erie Tobacco Co": ["Seneca"],
    "Nordic American": ["Klondike", "Nordic Ice"],
    "Rockbridge Tobacco": ["Tahoe"],
    "USSTC": ["Copenhagen", "Husky", "Red Seal", "Skoal", "Wb", "Verve"],
    "V2 Tobacco Co.": ["Thunder"],
    "Rogue Holdings": ["Rogue"],
    "Dryft Sciences": ["Dryft"],
    "Nicky Drops": ["Nicky Drops"],
    "Gel Group USA": ["Nicogel"],
    "Nicotek LLC": ["Nicopix", "Metro"],
    "Nicostics": ["Nicosticks"],
    "Nicquid": ["Nicquid"],
    "Helix Innovations LLC": ["On"],
    "Pixotine Products Inc": ["Pixotine"],
    "Smart Toothpicks": ["Smrt"],
    "Modoral Brands Inc": ["Velo"],
    "21 Century": ["21 Century Smk", "21 Century Smoke", "Vapin Plus"],
    "2D Enterprises": ["Evo", "EVO"],
    "7 Daze": ["7 Daze", "Red"],
    "8937001 Canada Inc.": ["Chill"],
    "Air Bar Vape Inc": ["Air Br"],
    "Air Factory E-Liquid": ["Air Factory", "Air Stix", "Trt Factory"],
    "Airistech": ["Airis"],
    "Airo": ["Airo"],
    "All Day Vapes": ["All Day Vapes"],
    "Allay E-Cigarette Inc": ["Allay"],
    "Alternative Smoking Solutions LLC": ["Next"],
    "Americana Tobacco Co": ["Americana"],
    "Apollo Future Technology Inc": ["Apollo"],
    "Aqua Ejuice": ["Aqua"],
    "Aqua Haze": ["Aqvaze"],
    "Aqua Salts": ["Aqua Salts"],
    "Aquasmoke": ["Aquasmk", "Aquasmoke"],
    "Arctic Smoke CSC LLC": ["Arctic"],
    "Aspire": ["Aspire"],
    "Atmos Nation LLC": ["Atmos"],
    "Axiocore Corporation": ["Yogi"],
    "Bacher Deland LLC": ["Blk Lb"],
    "Bad Drip Labs": ["Bad"],
    "Bad Modder Fogger Llc": ["Bad Modder Fogger"],
    "Bali Vape USA": ["Bali"],
    "Ballantyne Brands LLC": ["Mistic", "Neo"],
    "Banana Butt Eliquid": ["Banana Butt"],
    "Barista Brew Company": ["Barsta Brew"],
    "Bayou Swamp Juice Llc": ["Bayou Swamp"],
    "Beard Vape Co": ["Beard", "Pour House", "Punch", "Super Strudel", "The 1", "Vapor Maid"],
    "Bidi LLC": ["Bidi"],
    "Black Dragon Enterprises LLC": ["Green Leaf", "Gren Leaf", "Grn Leaf"],
    "Black Note": ["Black Note"],
    "Blaze": ["Blaze"],
    "Blis Ecig": ["Blis"],
    "Blitz Vapes": ["Blitz"],
    "Blow": ["Blow", "Vit Smk", "Vitamin Smoke", "Nat Vapes", "NV"],
    "Blvk Unicorn": ["Blvk Uncrn", "Blvk Unicorn"],
    "Bmor Vape Inc": ["Bmor"],
    "Bnb Enterprise": ["Zen"],
    "Bo Vaping": ["Bo Vaping", "Bo Vpng", "Ignite", "Panda Kush", "Bo Plus"],
    "Bold Vapor": ["Bld", "Bold"],
    "Boost Vape": ["Boost"],
    "Boss Hog Ejuice": ["Boss Hog"],
    "Boulder International Inc": ["Boldr", "Boulder"],
    "Breez Smoke LLC": ["Brz Smk"],
    "Buah Variable Voltage": ["Buah"],
    "Buck Naked": ["Buck Naked", "Buck Nkd"],
    "Bulldog": ["Bulldog"],
    "Burst E-Liquid": ["Burst"],
    "Buzz Ecigs": ["Lava"],
    "Cake Vapors": ["Cke"],
    "California Cloud Vapes": ["Cke"],
    "California Eliquids": ["Cali"],
    "Carolina Flavor Inc": ["Carolina Flavor", "Carolina Flvr", "Carolina Flavors"],
    "Charlie Noble": ["Charlie Noble"],
    "Charlies Chalk Dust": ["CCD", "Ccd", "Charlies Chalk Dust", "Creator Of Flavor", "Pacha Syn", "Pachamama"],
    "Cigees Ltd.": ["Ciggees"],
    "Cigirex LLC": ["Cigirex"],
    "Circus Cookie E-Liquid": ["Circus Ckie"],
    "Clean Cig": ["Clean Cig", "Cln Cig", "Ecigar"],
    "Cndy Eliquid": ["Cndy"],
    "Coastal Clouds Company": ["Coastl Cloud"],
    "Coil Butter": ["Coil Btr"],
    "Coil Glaze": ["Coil Glaze"],
    "Cool Breeze Vapor Llc": ["Cl Brz"],
    "Cool Vapor Llc": ["Cool Vapor"],
    "Cosmic Fog": ["Cosmic Fog", "The Lost Fog", "Whipd", "Xcel Sixty"],
    "Crave Disposable": ["Crave"],
    "Cravinvapes Corp": ["Abduct", "Arctic", "Blk Bird", "Boss Brew", "Cravinvapes", "Hardline", "Junk Food",
                         "Savage Sauce"],
    "Crisp": ["Crsp", "Crisp"],
    "Crown 7": ["Crown 7", "Spartan", "Sub Ohm"],
    "Cuttwood LLC": ["Cuttwood"],
    "Dark Market Vape Company": ["Dark Market", "Dk Mrkt"],
    "Dinner Lady Fam Limited Co": ["Dinner Lady", "Dnnr Lady", "Tornado"],
    "DNA Distributors Inc": ["Cigalectric"],
    "Dongguan Delin Technology": ["Juicy"],
    "Dream Bar": ["Drm Br"],
    "Drip Drop": ["Blossom", "Vague Something"],
    "Dripmore Ejuice": ["Apl Twst", "Ckie Kng", "Ckie Twst", "Cndy Kng", "Dripmore", "Frt Twst", "Hi Drip", "Hny Twst",
                        "Lemon Twist", "Lmn Twst", "Mlk Kng", "Mln Twst", "Trop Kng"],
    "Drizzle": ["Drizzle"],
    "E-Luminate E-Cigarettes": ["Elum", "E-Luminate"],
    "EAS": ["Cue", "Infuse", "Leap", "Liq"],
    "East Coast Company": ["Excel"],
    "Ecig Industries": ["Chaos"],
    "Eco Cigs INC": ["Eco Cig"],
    "Ecto World": ["Ecto"],
    "Egeneration": ["Egen", "Egnrtn", "Hollywood", "Kings Barrel", "Kngs Barrel"],
    "Eleaf": ["Eleaf", "Istick"],
    "Electric Lotus": ["Melon Heads"],
    "Elektro USA": ["Elektro"],
    "Elf Bar": ["Elf Br", "Lost Mary"],
    "Elite Ejuice": ["Elite"],
    "Elysian Labs": ["Elysian"],
    "Encore Vapor Inc": ["Bakd", "Encore"],
    "Envy Electronic Cigarettes": ["Spirit"],
    "Eonsmoke": ["4x Salts", "Eonsmk", "Eonsmoke"],
    "Epitome Eliquid": ["Epitome"],
    "Evolve": ["Evolve"],
    "Ezzy Oval": ["Ezzy Oval"],
    "Fanatics E Juice": ["Salt Fanatics"],
    "Fantasia Distribution Inc": ["Fantasia", "Hydro"],
    "FAZE USA INC.": ["Faze"],
    "Finiti Branding Group": ["Fin"],
    "Five Star Juices": ["5 Star", "Five Star"],
    "Fizz Vapors": ["Fizz", "Sheesh"],
    "Flair": ["Flair"],
    "Flowermate USA": ["Flowermate"],
    "Fliz": ["Fliq"],
    "Flo Ecigs": ["Flo Ecigs"],
    "Fogs Brew": ["Fogs Brew"],
    "Fontem US Inc": ["Blu", "Quik"],
    "Freedom Ecigs": ["Freedom Ecigs", "Smkr Friendly"],
    "Freedom Smokeless": ["Freedom"],
    "Fresh Farms E Liquid": ["Frsh Frm", "Fruitia"],
    "Fryd E-Liquids": ["Drip"],
    "Fum Electronic Vaporizers": ["Fum"],
    "Fuma International LLC": ["Fuma", "Posh"],
    "Fume LLC": ["Fume", "Fuzze"],
    "Fumee LLC": ["Fumee"],
    "Fusion Vape Inc.": ["Fusion", "Fusn"],
    "Gamucci": ["Gamucci"],
    "Geek Bar": ["Geek Br"],
    "Ghost Disposable": ["Ghost"],
    "Glas Llc": ["Glas"],
    "Globs Juice Company": ["Globs"],
    "Goo Sticks": ["Goo"],
    "Goodies Express": ["Happystiks"],
    "Green Planet Inc": ["Clic", "Magic Stk", "Vape Magic"],
    "Green Smart Living": ["Grn Smrt", "Grnsmart"],
    "Green Tree Syndicate Inc": ["Agent 009"],
    "Harlequinn Premium E-Juice": ["Harlequinn"],
    "Hato Vape": ["Hato"],
    "Hellvape": ["Dead Rabbit"],
    "Haze Tobacco LLC": ["Haze"],
    "Heavens Lube Junky": ["Junky"],
    "Hi-Tech Products LLC": ["Hi Tech"],
    "Hi5": ["Hi5"],
    "High Heat Co.": ["High Heat"],
    "Hit Stix": ["Hit Stix"],
    "Hitt Vape": ["Hitt"],
    "Honey Stick": ["Hny Stk"],
    "Hong Kong Snbg Limited": ["Wotofo"],
    "Horizon Technology Co Ltd": ["Horizon"],
    "Hqd Tech": ["Cuvie", "HQD", "Whiff"],
    "Humble Juice Co.": ["Humble", "Salt"],
    "Hybrid": ["Hybrid"],
    "Hyde Disposables": ["Hyde"],
    "Hyla USA": ["Hyla"],
    "Ice Cream Man E-Liquid": ["Gelato"],
    "Ice Rabbit": ["Ice Rabbit"],
    "Ijoygroup Company Ltd": ["Ijoy"],
    "Image Electronic Cigs": ["Image"],
    "Imperial Smoke": ["Imp Hkh", "Imperial Hkh"],
    "Independent Vapor Company": ["Pod"],
    "Innevape Labs Usa": ["Innevape", "The Berg"],
    "Innovape Tech": ["Innovape"],
    "Innokin Technology Inc": ["Innokin", "Itaste"],
    "Instabar": ["Instabar"],
    "Instacig": ["Instacig"],
    "International Vapor Group Inc.": ["Nutricigs", "Vaporfi"],
    "Ism Vape": ["Ism Vape"],
    "Ismok LLC": ["Ismok", " Ismoke", "iSmoke", "Smok"],
    "Ispire": ["Ispire"],
    "Jacksam Corp": ["Blkout Tob"],
    "Jak Ecig": ["Jak"],
    "Jam Monster Liquids": ["Crm Team", "Monstr"],
    "Jasper&Jasper": ["Jasper"],
    "Jazz E": ["Jazz E"],
    "Jb Vapor": ["Juice Bx"],
    "Jet Cigs LLC": ["Jet Cig"],
    "Jjuice": ["Juice"],
    "Jmas Enterprises Inc": ["Sq", "Square", "Starbuzz"],
    "Johnson Creek": ["Johnson Creek", "Kiln House", "Kiln Hse", "Red Oak"],
    "Joy Flavor": ["Joy Flavor"],
    "Jt Bruns & Crow Llc": ["Action Fluid"],
    "Juice Bomb": ["Juice Bomb"],
    "One Hit Wonder  Inc": ["1 Hit Wndr", "One Hit Wonder"],
    "Joyetech": ["Ego", "Elitar", "Joyetech"],
    "Juice Head E-Liquid": ["Juice Hd"],
    "Juicy Ejuice": ["Juicy"],
    "Jupiter": ["Jupiter"],
    "Justfog": ["Justfog"],
    "Jum Vapor": ["Jum"],
    "Just Juice Usa": ["Jst Juice"],
    "Juucy LLC": ["Juucy"],
    "Juul Labs": ["Juul"],
    "Kandypens": ["Kandypens"],
    "Kentucky Select": ["Ky Slct"],
    "Ki Group Usa LLC": ["Ki"],
    "Killa Fruits E-Juice": ["Jilla Frt"],
    "Kilo E-Liquid Inc": ["Kilo", "Sr Ser"],
    "King Pluto": ["Razzo"],
    "Kk Energy": ["Kk Enrgy"],
    "La Vapor Inc": ["Nic5"],
    "Laisimo": ["Starshot"],
    "Lem": ["Kandies", "Lem"],
    "Limitless Trading Company Llc": ["Drips"],
    "Lips and Drips": ["Lips And Drips"],
    "Liquid Labs": ["Keep It 100"],
    "Liquid State Vapors": ["Liq State", "Liquid State"],
    "Loaded": ["Glzd"],
    "Logic Technology": ["Logic", "Vapeleaf"],
    "Lost Art Liquids": ["Lost Art", "Slotter"],
    "Lost Vape": ["Orion", "Lost Vape"],
    "Lotus Electronic Cigarettes": ["Lotus", "Revd"],
    "Lucid": ["Lucid"],
    "Lush": ["Lush"],
    "Lust": ["Lust"],
    "Luto Vapor": ["Luto"],
    "Luxury Lites": ["Luxury Lites"],
    "Mad Hatter": ["I Love"],
    "Mad Hatter Juice": ["120 Crl", "Ade"],
    "Marina Vape": ["Aqua", "Creme de La Creme", "Crm De LA Crm", "Crm De La Crm", "Milkshake Man", "Stratus"],
    "Maxx Vapor": ["Maxx"],
    "Mecig": ["Mecig"],
    "Mega Vape LLC": ["Mega Vape"],
    "Met 4 Vapor Llc": ["Fairgrounds", "Gldn Ticket", "The Standard"],
    "Mfn Donut": ["Mfn Donut"],
    "Micro Brew Vapor": ["Mcr Brew Vapor"],
    "Midwest Goods Inc": ["Puff Buddi"],
    "Mighty Vapors": ["Might Vapors", "Nicsalts"],
    "Millennium Smoke": ["Mellennium"],
    "Mints Vape Co": ["Mnt"],
    "Mio Vapor": ["Mio"],
    "Mojo Vapor": ["Mojo"],
    "Monster Vape Labs": ["Cstrd Monstr", "Frt Monstr", "Ice Monstr", "Jam Monstr", "Lmnade Monstr", "Pb/Jam Monstr",
                          "The Mlk", "Tob Monstr"],
    "Mood Pods Ltd": ["Mood", "Smood"],
    "Moodtime": ["Moodtime"],
    "Mountain Vapor Llc": ["Airbender"],
    "Mr Good Vape": ["Mr Gd"],
    "Mr Salt E": ["Mr Salt E", "Mr Slt E"],
    "Mrktplce Eliquid": ["Bkrs Bskt", "Mrktplce"],
    "Mv Enterprises Llc": ["Cloud Nurdz"],
    "Myle Vapor": ["Myle"],
    "Myst Enterprises Inc": ["My Myst", "Myst"],
    "My My Vape": ["My Vape"],
    "Naked Fish": ["Naked Fish", "Nkd Fish"],
    "Newhere Inc": ["Newhere"],
    "Nexus Vapor": ["Nicmaxx"],
    "Nicstick E-Cigarette Corp": ["Nicstick"],
    "Niin LLC": ["Niin"],
    "NJOY": ["NJoy"],
    "Noble Vapors": ["Noble"],
    "Northwest Vapors LLC": ["Northwest Vapors", "Summit", "Taste"],
    "Nu Mark": ["Green Smoke", "Grn Smk", "MarkTen"],
    "Nu1s": ["Nu1s"],
    "Nubilus": ["Nubilus"],
    "NXGP Llc": ["Vanish"],
    "Oem Partners LLC": ["Reve"],
    "Off The Record Liquids": ["Off The Record"],
    "Og Distribution": ["The Hppy"],
    "Okvmi Brand": ["Lit"],
    "Ooze Inc": ["Ooze"],
    "Ovns": ["Ovns"],
    "Oxva": ["Oxva"],
    "One Hit Wonder Liquid": ["Famous Fair"],
    "Opma Project": ["Opmh", "The Fountain Blast"],
    "Ozone Smoke": ["Ozone Smk"],
    "Pax Labs": ["Pax"],
    "Ploom": ["Ploom"],
    "Paradym Ecigs": ["Paradym"],
    "Pastel Cartel": ["Esco"],
    "Phillips & King": ["Spellbound"],
    "Philly Vape Society": ["Sadboy Tear"],
    "Phix Vapor": ["Infzn", "Phix"],
    "Pinup Vapors": ["Pinup"],
    "Playboy Vapor Collection": ["Playboy Vapor Colltn"],
    "Pomme Ejuice": ["Pomme Bon Bon"],
    "Pop Clouds E-Liquid": ["Pop Clouds"],
    "Pop Pods": ["Pop"],
    "Private Label Manufacturer": ["Delta Diamond", "Delta Dmnd"],
    "Propaganda E-Liquid Llc": ["Propoganda", "The Hype"],
    "Provape Enterprise Inc": ["Puff Xtra"],
    "Psycho Tunes": ["Psycho Tunes"],
    "Puf Cigs Inc": ["Puf"],
    "Puff Brands": ["Hot Bx"],
    "Puff E-Cig Inc": ["Ezzy", "Puf", "Puff"],
    "Puff Labs": ["Puff"],
    "Puffco": ["Puffco"],
    "Pulsar Vaporizers": ["Pulsar"],
    "Pure Salt Eliquid": ["Pure Salt", "Pure Slt"],
    "Pure Vapor": ["Pure"],
    "Puresmoke LLC": ["Puresmk", "Puresmoke"],
    "Puro": ["Puro"],
    "Purre E-Cigarettes": ["Purre"],
    "Puur Smoke": ["Puur"],
    "Randmvape": ["R/M"],
    "Randys Marketing Group": ["Randys"],
    "Real Feel": ["Real Feel"],
    "Red Dawn Vapes": ["Red Dawn"],
    "Red Star E Liquid": ["Red Star"],
    "Republic Brands": ["Real"],
    "Ripe Vapes": ["Ripe Vapes"],
    "Ritchy Group Ltd": ["Liqua"],
    "RJR Vapor Company": ["Vuse"],
    "Rolled Green": ["Rolled Grn"],
    "Romeo Group Co Ltd": ["Hyppe"],
    "Roor": ["Roor"],
    "Rounds E-Liquid": ["Something"],
    "Roy Vapry": ["Roy Vapry"],
    "Rsc LLC": ["Etc"],
    "Ruthless Vapors": ["Ruthless", "Str8"],
    "Salt Lake": ["Slt Lake"],
    "Sago Technology": ["Epic"],
    "Saltbae50": ["Saltbae50"],
    "Saltnic Inc": ["VGod"],
    "Salty Man Vape Ejuice": ["Salty Man", "Slty Man"],
    "Saucy Group": ["Saucy"],
    "Savage E-Liquid": ["Savage Vape"],
    "Save A Smoker": ["Save A Smkr", "Save A Smoker"],
    "Saveurvape Inc": ["Bad Joos", "Clapback", "Met 4", "Svrf", "SVRF"],
    "Sense Technology Co": ["Linkedvape", "Sense"],
    "Shenzhen Artery Technology Co Ltd": ["Aramax", "Smpo", "Artery", "Movkin", "Vaporesso"],
    "Shenzhen Asvape Technology Co": ["Asvape"],
    "Shenzhen Dovpo Technology Co": ["Dovpo"],
    "Shenzhen Daotong Electronics Co Ltd": ["Glamee"],
    "Shenzhen Kangvape Technology Co Ltd": ["Smod", "Kangertech"],
    "Shenzhen Pinrui Industrial Co Ltd": ["Luckee"],
    "Shenzhen Obs Technology Co": ["Obs"],
    "Shenzhen Smoore Technology Limited": ["Vaporesso"],
    "Shenzhen Uwell Technology Co": ["Uwell"],
    "Shijin Vapor": ["Bolt", "Shijin"],
    "Shirley And Roy": ["Shirley"],
    "Sigelei": ["Sigelei"],
    "Silver Moon Nutraceuticals": ["Kavanol"],
    "Silverback Juice Co": ["Silverback"],
    "Sky Bar Vapes LLC": ["Sky Br"],
    "Sky Pods": ["Sky Pod"],
    "Smok Technology Co": ["Fyre Ban", "Nord Turbo", "Novo", "Smok"],
    "Smoke": ["Smk", "Smoke"],
    "Smoke Free": ["Smk Free"],
    "SmokeEnds": ["Smkend", "Smokeends"],
    "Smoker Friendly International": ["Sfi", "Smoker Friendly"],
    "Smokestik": ["Smkstik"],
    "Smoking Vapor": ["Mi Salt", "Mi-One", "Mi Pod", "Wi Pod"],
    "Smooth Cigs": ["Smooth", "Smth"],
    "Smoq Disposable": ["Smoq"],
    "Sol Vapor Usa Inc": ["Sol"],
    "Solace Vapor": ["Solace"],
    "Sonic Vape": ["Sonic"],
    "South Beach Smoke": ["Sth Beach"],
    "Space Jam Juice Llc": ["Space Jame", "Space Jam"],
    "Spark Industries LLC": ["Blind Lion", "Cig2o", "Vapage"],
    "SS Choice LLC": ["7s"],
    "Starbuzz Tobacco In": ["Starbuzz"],
    "Stay Salty E Liquid": ["Stay Salty"],
    "Steam Engine Vape": ["Vape Monstr"],
    "Suicide Bunny": ["Suicide Bunny", "Suicide Bnny", "Honey Bear", "Cloud Company", "The Cloud Co"],
    "Suorin": ["Suorin"],
    "Superb Liquids": ["Superb"],
    "Supreme Electronic Cigarette": ["Suprm"],
    "Sweet Southern Vapes": ["Swt Sthrn"],
    "SX Brands": ["Criss Cross", "Sthrn Steel", "Sthrn Stl"],
    "Stok": ["This Thing Rips"],
    "Td Distro": ["Lollidrip"],
    "Teardrip Juice Co": ["Teardrip"],
    "Teemo": ["Apoc", "Teemo"],
    "Th3riac": ["Th3riac"],
    "The Art Of Eliquids": ["2bacco", "Big Jugs", "Smoothy", "Smthy Man", "The Cstrd Shop", "Cndy Shop"],
    "The Good Crisp Company": ["Baton"],
    "The Council Of Vapor": ["Mini Volt"],
    "The Kind Group LLC": ["Kind"],
    "The Kind Pen": ["Kind"],
    "The Halo CO": ["Halo"],
    "The Jones": ["The Jones"],
    "The Makers Of Lemon Twist": ["Oro"],
    "The Schwartz": ["Nkd"],
    "The Vapors Knoll": ["Vapor Knoll"],
    "Three Monkeys": ["Mnky Bizznss", "Thr33 Monkyz"],
    "Thrive Liquid": ["Thrive"],
    "Tots": ["Tots"],
    "Transistor Ejuice": ["Transistor"],
    "Tripl3 Ecig": ["Tripl3", "Trpl3", "Triple3"],
    "Trulybar": ["Trulybar"],
    "Tryst Inc": ["Tryst"],
    "Tsunami": ["Tsunami"],
    "Twelve Vapor": ["Juno", "Twelve"],
    "Twist E-Liquids": ["360", "Twst"],
    "Ultraflow": ["Ultraflow"],
    "Uncle Junks Genius Juice": ["Uncle Junks"],
    "Undetermined Mfg": ["1", "3X", "A1", "Acacia", "Air", "Alphaa", "Amo Pod", "Amigo", "Aqvaze",
                         "Atomc Cloud", "Athenaz", "Atomic Cloud", "Baba Delta", "Coilart", "E Cloud", "E8"
                         "Bang", "Barz", "Bidi", "Biffbar", "Big Boy", "Blndd", "Bloko", "Bloom", "Blvd", "Bomb",
                         "Breakfast Club", "Brz Smk", "Bud Vape", "Burlesque", "Buz", "C4", "Caesar", "Capt Fog",
                         "Carbn", "Chain Vapez", "Cke Delta", "Cl Pod", "Cloud", "Cloudmouth", "Clown", "Core",
                         "Cosmonaut", "Crave", "Crazy", "Crossbar", "Crush", "Crushed", "Cutie Mandarin",
                         "Dead Prezidents", "Delicious", "Desert Sun", "Devotion", "Dmnd Xtrm", "Donuts", "Doo",
                         "Dr Fog", "Drag", "Dragon Monstr", "Drip", "Drm", "Elite", "Epe", "Evaze", "Ezee", "Face",
                         "Fira", "Fiya", "Flex", "Floom", "Flum", "Foger", "Fresh", "Freemax", "Frsh", "Fuego", "Fum",
                         "Glace", "Geek Vape", "Go Smoke", "Helix", "Hero", "Hit", "Huge", "Hyde", "Hyppe",
                         "Infusion", "Isalt", "Imini", "Jem", "Jomotech", "Joystick", "Jst", "Juccypod", "Jump Stars",
                         "Jus", "Just", "Jvb", "Kado", "Kangvape", "Kiki", "KY", "Le", "Leef", "Litz", "Lolli", "Loon",
                         "Loosie", "Loy", "Mamba", "Melange", "Mngo", "Moti", "Mr", "MrFog", "Mr Fog", "My Br", "Nanas",
                         "Nano", "Nero", "Nasty", "Neon", "Nic Br", "Nomenon", "Old Fashioned", "Old Fshn", "Pablo",
                         "Packspod", "Phresh", "Phyre", "Plus", "Pluto", "Poco", "Pod", "Prism", "Pro", "Prophet",
                         "Prohibited", "Pur", "Puff", "Quick Draw", "Rage", "Rincoe", "Shanlaan"
                         "Rare", "Rps", "Runningman", "Rush", "Ryse", "Sd", "Sea", "Shmizz", "Silky", "Simba", "Skol",
                         "Slaps", "Smack", "Smooth", "Smth", "Space", "Spacebar", "Stardust", "Steam Engine",
                         "Stokes", "Strm Chaser", "Suprm Saltz", "Sutta", "Swft", "Switch", "Taco", "Targa",
                         "The Colltn", "The Finest", "Tkca Squid", "To The Max", "Tugpod", "Twst", "Uac", "Ugly",
                         "Underestimated", "Unique", "Vape", "Vapmod", "Vivi", "Vapesoul", "Vaporlax", "Vcan", "Vector",
                         "Vfeel", "Vfun", "Vibe", "Viigo", "Viva", "Wismec", "Xtra", "Xx", "X20", "XX", "Yaya",
                         "Yuoto"],
    "United Tobacco Vapor Group": ["Flvr Vapes", "Prem Vapes", "Premium Vapes"],
    "US Vapor Corp": ["Elite"],
    "Valgous Inc": ["Blng", "Hyde"],
    "Vandy": ["Vandy"],
    "Vape Gear": ["Vape"],
    "Vape Storm Labs Llc": ["Vape"],
    "Vape Wulf": ["Vape"],
    "Vapioneer": ["Vapioneer"],
    "Vapir": ["Vapir"],
    "Vapmor": ["Vapmor"],
    "Vanguard": ["Screaming Eagle"],
    "Vapage Premium E-Cigarettes": ["Private Reserve"],
    "Vape Craft Inc": ["Vape Craft"],
    "Vape Shore Drive": ["Glazed Glazed"],
    "Vape Usa": ["Vape USA"],
    "Vape Wild": ["Vape Wld"],
    "Vapegoons": ["Vapegoons"],
    "Vapergate": ["Vapergate"],
    "Vapetasia": ["Vapetasia"],
    "Vapin' Goodies": ["Vapin Goodies"],
    "Vapor Blends Inc": ["Vapor"],
    "Vapor Corp": ["Krave", "Vaporx"],
    "Vapor Factory": ["Vapor Factory"],
    "Vapor Tech Usa": ["Vapor Tech"],
    "Vapor Tobacco Manufacturing Llc": ["Etron"],
    "Vapor USA Wholesale": ["Smkr Friendly"],
    "Vapor Dna": ["Vladdin"],
    "Vaporesso": ["Puffmi"],
    "Vaporillo Inc": ["Vaporillo"],
    "Vaporin LLC": ["Vaporin"],
    "Vaportronix": ["VQ"],
    "Vaporotics": ["E-Slick"],
    "Vaporzx Inc": ["Vaporzx", "VX Brandz"],
    "Vaptio Co": ["Beco", "Vaptio"],
    "Vapure": ["Stickylips"],
    "Victory Vapor Inc": ["Vzone"],
    "Ventura Vapor Company": ["Klir", "Liq Zoo"],
    "Vfeel": ["Sili Bx"],
    "Vgod Inc": ["Nic Salt"],
    "Victory Ecigs": ["Victory"],
    "Vigilante Juice Co.": ["Vigilante"],
    "VMR Products LLC": ["V2"],
    "Volcano Fine Electronic Cigs": ["Volcano"],
    "Von Erl": ["My Von Erl"],
    "Voopoo": ["Voopoo"],
    "Voyage Vapor": ["Voyage"],
    "Westside Vapor": ["Westside"],
    "White Rino": ["White Rhino", "Wht Rino"],
    "Worldwide Distribution": ["Clayton"],
    "Wyatt & Coopers Ltd": ["Cigma"],
    "Xen Bar": ["Xen Br"],
    "Xfire": ["X Fire"],
    "Xl Vape": ["Stig"],
    "Yocan Technology Company Ltd": ["Yocan"],
    "Yme Vape": ["Unc Qb"],
    "Zalt Nic Salts": ["Zalt"],
    "Ziip Lab": ["Cali", "Mr Puff", "Ziip Lab", "Zpods", "Zstick"],
    "Zoom E-Cigs LLC": ["Zoom"],
    "Zoor Vapor": ["Zoor"],
    "Any Vape": ["Anyvape"],
    "Atmanmarket": ["Atman"],
    "Atom Kit": ["Atom Kit"],
    "Big D Vapor": ["Donner"],
    "Chongson": ["Chongs"],
    "Direct Vapor": ["Nextmind"],
    "Extract Solutions Co": ["Extract Solutions"],
    "Exxus": ["Exxus"]

}

MANUFACTURER_PARENT_MAP = {
    "PM USA": "Altria Group, Inc."
}

PACK_TYPES = ["soft", "box",
              "foil", "pouch", "v-fresh", "up", "jar", "tin", "bx",
              "can", "tub",
              "pouch"]

CIGARETTE_PRODUCT_SIZES = ["70", "72", "83", "90", "99", "100", "101", "120", "king"]

CIGAR_PRODUCT_SIZES = ["cigarillos", "king"]

SMOKELESS_PRODUCT_SIZES = ["1.2oz", ".82oz", "1.33oz", "6oz", ".68oz", "7.2oz", ".60oz", ".47oz", "14.4oz", "12oz",
                           ".53oz", ".85oz", ".33oz", ".423oz", ".98oz", "1.5oz", ".49oz", ".44oz"]

TDP_PRODUCT_SIZES = []

TEXT_TRANSFORM_MAP = {
    "72mm": "72",
    "DK": "Dark",
    "Dk": "Dark",
    "Drk": "Dark",
    "Exp": "Express",
    "ExtraSmth": "Extra Smooth",
    "Gld": "Gold",
    "Grn": "Green",
    "Hgh": "High",
    "Jad": "Jade",
    "Men": "Menthol",
    "NF": "Non-Filter",
    "Orng": "Orange",
    "PRMR": "Premiere",
    "Pltnm": "Platinum",
    "SNTFE": "Santa Fe",
    "Smth": "Smooth",
    "Spcl": "Special",
    "St": "State",
    "SuperSmth": "Super Smooth",
    "UltSmth": "Ultra Smooth",
    "Yllw": "Yellow",
    "Bx": "box",
    "Cgrllo": "Cigarillos",
    "Blbry": "Blueberry",
    "Pinapl": "Pineapple",
    "Mln": "Melon",
    "Wht": "White",
    "Grp": "Grape",
    "Wtrmln": "Watermelon",
    "Swt": "Sweet",
    "Dmnd": "Diamond",
    "Kng": "King",
    "Nat": "Natural",
    "Rssn": "Russian",
    "Crm": "Cream",
    "Arma": "Aroma",
    "WdTip": "WoodTip",
    "Slvr": "Silver",
    "Strbry": "Strawberry",
    "Up": "Upright",
    "Jmcn": "Jamaican",
    "Pckt": "Packet",
    "Cgnc": "Cognac",
    "Fltr": "Filter",
    "Sprtsmn": "Sportsman",
    "Mdro": "Maduro",
    "Anstte": "Anisette",
    "Bourbn": "Bourbon",
    "FltrTip": "FilterTip",
    "Contntl": "Continental",
    "Orig": "Original",
    "Chrchll": "Churchill",
    "Grendr": "Grenadier",
    "Hny": "Honey",
    "Plstctip": "PlasticTip",
    "PlstcTip": "PlasticTip",
    "Wld": "Wild",
    "Teq": "Tequila",
    "Apl": "Apple",
    "Mngo": "Mango",
    "Chce": "Choice",
    "Crystl": "Crystal",
    "Blk": "Black",
    "Crona": "Corona",
    "Mld": "Mild",
    "Blnd": "Blend",
    "Club": "Club",
    "Mx": "Mix",
    "Clsc": "Classic",
    "Dlx": "Deluxe",
    "Gldn": "Golden",
    "Smmr": "Summer",
    "Strght": "Straight",
    "Pwr": "Power",
    "Lbl": "Label",
    "Chr": "Cherry",
    "Blkstone": "Blackstone",
    "Pch": "Peach",
    "Blnt": "Blunt",
    "Mstr": "Master",
    "Trpl": "Triple",
    "Frnch": "French",
    "Crmy": "Creamy",
    "Ccnut": "Coconut",
    "Capt": "Captain",
    "Frsco": "Fresco",
    "Tropical": "Tropical",
    "Cgr": "Cigar",
    "Islnd": "Island",
    "Dbl": "Double",
    "Crnbry": "Cranberry",
    "Atomc": "Atomic",
    "Fusn": "Fusion",
    "Mtn": "Mountain",
    "Royl": "Royal",
    "Hnycmb": "Honeycomb",
    "Dtch": "Dutch",
    "Cmroon": "Cameroon",
    "Colltn": "Collection",
    "Prfcto": "Perfecto",
    "Prsdnte": "Presidente",
    "Sprt": "Spirit",
    "Cnnssr": "Connoisseur",
    "Exec": "Executive",
    "Brnch": "Branch",
    "Grnd": "Grand",
    "Pntla": "Panatela",
    "Thrwout": "Throwout",
    "Rbsto": "Robusto",
    "Lnsdl": "Lonsadale",
    "Whpd": "Whipped",
    "Eng": "English",
    "Wntrmns": "Wintermans",
    "Psnfrt": "Passionfruit",
    "Jwl": "Jewel",
    "Frt": "Fruit",
    "Prpl": "Purple",
    "Extc": "Exotic",
    "Mxd": "Mixed",
    "Ychtmstr": "Yachtmaster",
    "Hse": "House",
    "Imp": "Imperial",
    "Flvr": "Flavor",
    "Mtnr": "Mountaineer",
    "Styl": "Style",
    "Lng": "Long",
    "Lt": "Light",
    "Mgnm": "Magnum",
    "Elctrc": "Electric",
    "Lf": "Life",
    "Splsh": "Splash",
    "Rl": "Roll",
    "Cmfrt": "Comfort",
    "Smkr": "Smoker",
    "Suprm": "Supreme",
    "Nuggt": "Nugget",
    "Velvt": "Velvet",
    "Blizz": "Blizzard",
    "Clb": "Club",
    "Trdr": "Trader",
    "Ult": "Ultra",
    "FC": "Fine Cut",
    "LC": "Long Cut",
    "WC": "Wide Cut",
    "Wntrgrn": "Wintergreen",
    "Mnt": "Mint",
    "Amer": "American",
    "Pmnt": "Peppermint",
    "Swts": "Sweets",
    "Vlue": "Value",
    "Ctrs": "Citrus",
    "Gen": "General",
    "Sprmnt": "Spearmint",
    "Bry": "Berry",
    "Brndy": "Brandy",
    "Dsslvbl": "Dissolvable",
    "Loz": "lozenge",
    "Gum": "gummies",
    "Cinn": "cinnamon",
    "Coff": "coffee",
    "Tthpck": "toothpick",
    "Tob": "tobacco",
    "Hrvst": "harvest",
    "Sft": "soft",
    "Frsh": "fresh",
    "Mffn": "muffin",
    "Pmpkin": "pumpkin",
    "Rspbry": "raspberry",
    "Blkbry": "blackberry",
    "Crml": "caramel",
    "Choc": "chocolate",
    "Cinmon": "cinnamon",
    "Stbry": "strawberry",
    "Gva": "guava",
    "Cdny": "candy",
    "Lmn": "lemon",
    "Lmnade": "lemonade",
    "Frzn": "frozen",
    "Pinclda": "pinacolada",
    "Ccnut": "coconut",
    "Passn": "passion",
    "Pomgrnt": "pomegranate",
    "Bbl": "bubble",
    "Hnydew": "honeydew",
    "Crnapl": "cranapple",
    "Mple": "maple",
}
