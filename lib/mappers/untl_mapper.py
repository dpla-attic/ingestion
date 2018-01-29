from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mapper import Mapper

class UNTLMapper(Mapper):
    def __init__(self, provider_data, key_prefix="untl:"):
        super(UNTLMapper, self).__init__(provider_data, key_prefix)
        self.root_key = "metadata/metadata/"
        self.rights_term_label = {
            "by": "Attribution.",
            "by-nc": "Attribution Noncommercial.",
            "by-nc-nd": "Attribution Non-commercial No Derivatives.",
            "by-nc-sa": "Attribution Noncommercial Share Alike.",
            "by-nd": "Attribution No Derivatives.",
            "by-sa": "Attribution Share Alike.",
            "copyright": "Copyright.",
            "pd": "Public Domain."
        }
        self.dataprovider_term_label = {
            "AAAUT": "Alexander Architectural Archive, University of Texas at "
                     "Austin",
            "ABB": "Bryan Wildenthal Memorial Library (Archives of the Big "
                   "Bend)",
            "ABPL": "Abilene Public Library",
            "ABPM": "Abilene Philharmonic",
            "ACGS": "Anderson County Genealogical Society",
            "ACHC": "Anderson County Historical Commission",
            "ACRM": "Amon Carter Museum",
            "ACTUMT": "Archives of the Central Texas Conference United "
                      "Methodist Church",
            "ACUL": "Abilene Christian University Library",
            "AHS": "Arlington Historical Society’s Fielder House Museum",
            "ALC": "Abilene Library Consortium",
            "AMKV": "Texas A&M University Kingsville",
            "AMPL": "Amarillo Public Library",
            "AMS": "Alvin Museum Society",
            "AMSC": "Anne and Mike Stewart Collection",
            "APL": "Alvord Public Library",
            "ARCHPL": "Archer Public Library",
            "ARMCL": "Richard S. and Leah Morris Memorial Library",
            "ARMCM": "Armstrong County  Museum",
            "ARPL": "Arlington Public Library",
            "ASPL": "Austin History Center, Austin Public Library",
            "ASTC": "Austin College",
            "ATLANT": "Atlanta Public Library",
            "ATPS": "Austin Presbyterian Theological Seminary",
            "AUSML": "Austin Memorial Library",
            "BACHS": "Bartlett Activities Center and the Historical Society "
                     "of Bartlett",
            "BANDPL": "Bandera Public Library",
            "BAYLOR": "Baylor University",
            "BCHC": "Bosque County Historical Commission",
            "BDPL": "Boyce Ditto Public Library",
            "BECA": "Beth-El Congregation Archives",
            "BEHC": "Bee County Historical Commission",
            "BHML": "Butt-Holdsworth Memorial Library",
            "BHS": "Bellaire Friends Library & Historical Society",
            "BIRD": "Birdville Historical Society",
            "BONPL": "Bonham Public Library",
            "BOWPL": "Bowie Public Library",
            "BPL": "Lena Armstrong Public Library",
            "BRIT": "Botanical Research Institute of Texas Library",
            "BROWN": "Brown County Museum of History",
            "BRPL": "Breckenridge Public Library",
            "BSAM": "Boy Scouts of America National Scouting Museum",
            "BSTPL": "Bastrop Public Library",
            "BUCHC": "Burnet County Historical Commission",
            "BURPL": "Burleson Public Library",
            "BWPL": "Bell/Whittington Public Library",
            "CAH": "The Dolph Briscoe Center for American History",
            "CAL": "Canyon Area Library",
            "CAMWL": "Carl and Mary Welhausen Library",
            "CARCL": "Carson County Library",
            "CASML": "Casey Memorial Library",
            "CCGS": "Collin County Genealogical Society",
            "CCHC": "Cherokee County Historical Commission",
            "CCHM": "North Texas History Center",
            "CCHS": "Clay County Historical Society",
            "CCLC": "Chambers County Library System",
            "CCMH": "Childress County Heritage Museum",
            "CCMS": "Corpus Christi Museum of Science and History",
            "CCPL": "Corpus Christi Public Library",
            "CCS": "City of College Station",
            "CCTX": "City of Clarendon",
            "CELPL": "Celina Area Historical Association",
            "CGHPC": "City of Granbury Historic Preservation Commission",
            "CHM": "Clark Hotel Museum",
            "CHMH": "Cedar Hill Museum of History",
            "CHOS": "Courthouse-on-the-Square Museum",
            "CHRK": "Cherokeean Herald",
            "CKCL": "Cooke County Library",
            "CLHS": "Cleveland Historic Society",
            "CLIFNE": "Nellie Pederson Civic Library",
            "CLMNPL": "Coleman Public Library",
            "CLTU": "Coates Library, Trinity University",
            "CODY": "Private Collection of Cody Garcia",
            "COLPL": "The Colony Public Library",
            "COMPL": "Comanche Public Library",
            "CPBM": "Cleng Peerson Research Library at the Bosque Museum",
            "CPL": "Carrollton Public Library",
            "CPPL": "Cross Plains Public Library",
            "CPS": "Cistercian Preparatory School",
            "CRPL": "Crosby County Public Library",
            "CSRHC": "Cherokee Strip Regional Heritage Center",
            "CTLS": "Central Texas Library System",
            "CTRM": "Cattle Raisers Museum",
            "CUA": "Concordia University Texas",
            "CUERPU": "Cuero Public Library",
            "CVPL": "Castroville Public Library",
            "CWCM": "Collingsworth County Museum",
            "CWTC": "Cowtown Coliseum",
            "CZECH": "Czech Ex-Students Association of Texas",
            "DAPL": "Dallas Public Library",
            "DCCCD": "Dallas County Community College District",
            "DCPL": "Delta County Public Library",
            "DENN": "Dennis M. O’Connor Public Library",
            "DEPOT": "Depot Public Library",
            "DFFM": "Dallas Firefighters Museum",
            "DGS": "Dallas Genealogical Society",
            "DHPS": "Danish Heritage Preservation Society",
            "DHS": "Dallas Historical Society",
            "DHVG": "Dallas Heritage Village",
            "DISCO": "UNT Digital Scholarship Cooperative (DiSCo)",
            "DISD": "Denton Independent School District",
            "DMA": "Dallas Museum of Art",
            "DPHF": "Dr. Pound Historical Farmstead",
            "DPKR": "City of Denton Parks and Recreation",
            "DPL": "Denton Public Library",
            "DSCL": "Deaf Smith County Library",
            "DSMA": "Dallas Municipal Archives",
            "DUBPU": "Dublin Public Library",
            "DUVAC": "Duval County Library",
            "EACM": "Eastland Centennial Memorial Library",
            "ECGS": "Erath County Genealogical Society",
            "EDHR": "Ed & Hazel Richmond Public Library",
            "EFNHM": "Elm Fork Natural Heritage Museum",
            "EHC": "Westbank Community Library District",
            "ELECPL": "Electra Public Library",
            "ELLIM": "Ellis Memorial Library",
            "ELPL": "El Paso Public Library",
            "EMT": "Edison Museum",
            "ENPL": "Ennis Public Library",
            "EPCHS": "El Paso County Historical Society",
            "EPL": "Euless Public Library",
            "ETGS": "East Texas Genealogical Society",
            "FBCL": "Fort Bend County Libraries",
            "FBCM": "FBC Heritage Museum",
            "FBM": "Fort Bend Museum",
            "FCCA": "First Christian Church of Arlington",
            "FCHC": "Fannin County Historical Commission",
            "FCPA": "First Christian Church of Port Arthur",
            "FELCG": "First Evangelical Lutheran Church of Galveston",
            "FMBRL": "FM Buck Richards Library",
            "FMPL": "Flower Mound Public Library",
            "FMTX": "Fire Museum of Texas",
            "FPL": "Ferris Public Library",
            "FRISCO": "Frisco Public Library",
            "FRLM": "French Legation Museum",
            "FRNTX": "Frontier Texas!",
            "FSML": "Friench Simpson Memorial Library",
            "FWAM": "Fort Worth Aviation Museum",
            "FWHC": "The History Center",
            "FWJA": "Fort Worth Jewish Archives",
            "FWMSH": "Fort Worth Museum of Science and History",
            "FWPL": "Fort Worth Public Library",
            "GAINES": "Gaines County Library",
            "GCFV": "Grayson County Frontier Village",
            "GCHS": "Gillespie County Historical Society",
            "GEPFP": "George Everill Pierce Family Photographs",
            "GHF": "Galveston Historical Foundation",
            "GHS": "Georgetown Historical Society",
            "GIBBS": "Gibbs Memorial Library",
            "GJRL": "Gladys Johnson Ritchie Library",
            "GLO": "Texas General Land Office",
            "GMHP": "Genevieve Miller Hitchcock Public Library",
            "GPHO": "Grand Prairie Historical Organization",
            "GR": "George Ranch Historical Park",
            "GRACE": "The Grace Museum",
            "GRVINE": "City of Grapevine",
            "HARPL": "Harper Library",
            "HCDC": "Henderson County District Clerk's Office",
            "HCGS": "Hutchinson County Genealogical Society",
            "HCLB": "Hutchinson County Library, Borger Branch",
            "HCLY": "Hemphill County Library",
            "HCPL": "Harris County Public Library",
            "HGPL": "Honey Grove Preservation League",
            "HHCT": "Hispanic Heritage Center of Texas",
            "HHSM": "Heritage House Museum",
            "HIGPL": "Higgins Public Library",
            "HMDTC": "Temple College",
            "HMRC": "Houston Metropolitan Research Center at Houston Public "
                    "Library",
            "HOCOG": "Hopkins County Genealogical Society",
            "HOND": "Hondo Public Library",
            "HPGML": "Dr. Hector P. Garcia Memorial Library",
            "HPL": "Houston Public Library",
            "HPUL": "Howard Payne University Library",
            "HPWML": "Harrie P. Woodson Memorial Library",
            "HSCA": "Harris County Archives",
            "HSUL": "Hardin-Simmons University Library",
            "HTPL": "Haslet Public Library",
            "HUMBM": "Humble Museum",
            "IPL": "Irving Archives",
            "ITC": "University of Texas at San Antonio Libraries Special "
                   "Collections",
            "JCGS": "Johnson County Genealogical Society",
            "JCML": "Jackson County Memorial Library",
            "JFRM": "Jacob Fontaine Religious Museum",
            "JSNPL": "Jacksonville Public Library",
            "KCHC": "Kerr County Historical Commission",
            "KCT": "Killeen City Library System",
            "KEMPF": "Harris and Eliza Kempner Fund",
            "KERPL": "Kerens Public Library",
            "KHSY": "Kemah Historical Society",
            "KUMC": "Krum United Methodist Church",
            "LAMAR": "Lamar State College – Orange",
            "LAMPL": "Lampasas Public Library",
            "LAMU": "Lamar University",
            "LANC": "Lancaster Genealogical Society",
            "LBJSM": "LBJ Museum of San Marcos",
            "LCBT": "Lee College",
            "LCHHL": "League City Helen Hall Library",
            "LCVG": "Log Cabin Village",
            "LDMA": "Lockheed Martin Aeronautics Company, Fort Worth",
            "LHS": "Lubbock High School",
            "LLAN": "Llano County Public Library",
            "LOGR": "The Library of Graham",
            "LPL": "Laredo Public Library",
            "LSCTL": "LSC - Tomball Community Library",
            "LVPL": "Longview Public Library",
            "MAMAV": "Maria-Martha Verin",
            "MAMU": "Mexic-Arte Museum",
            "MARD": "Museum of the American Railroad",
            "MATTA": "Private Collection of Matta Family",
            "MBIGB": "Museum of the Big Bend",
            "MCCHC": "McCulloch County Historical Commission",
            "MCGM": "McGinley Memorial Public Library",
            "MCMPL": "McAllen Public Library",
            "MERPL": "Meridian Public Library",
            "MESQUT": "Historic Mesquite, Inc.",
            "MFAH": "The Museum of Fine Arts, Houston",
            "MFWHM": "McFaddin-Ward House Museum",
            "MGC": "Museum of the Gulf Coast",
            "MHSI": "Murphy Historical Society Inc.",
            "MINML": "Mineola Memorial Library",
            "MLCC": "Matthews Family and Lambshead Ranch",
            "MML": "Livingston Municipal Library",
            "MMLUT": "Moody Medical Library, UT",
            "MMMM": "Medicine Mound Museum",
            "MMPL": "Moore Memorial Public Library",
            "MMUL": "McMurry University Library",
            "MONT": "Montgomery County Memorial Library",
            "MPLI": "Marshall Public Library",
            "MPPL": "Mount Pleasant Public Library",
            "MQPL": "Mesquite Public Library",
            "MRPCA": "Maverick Region Porsche Club of America",
            "MRPL": "Marfa Public Library",
            "MSTH": "Museum of South Texas History",
            "MWHA": "Mineral Wells Heritage Association",
            "MWSU": "Midwestern State University",
            "NCHC": "Newton County Historical Commission",
            "NCWM": "National Cowboy and Western Heritage Museum",
            "NELC": "Northeast Lakeview College",
            "NML": "Nesbitt Memorial Library",
            "NMPW": "National Museum of the Pacific War/Admiral Nimitz "
                    "Foundation",
            "NPSL": "Nicholas P. Sims Library",
            "OCHS": "Orange County Historical Society",
            "OJAC": "The Old Jail Art Center",
            "OKHS": "Oklahoma Historical Society",
            "ORMM": "The Old Red Museum",
            "OSAGC": "Old Settler's Association of Grayson County",
            "OTHER": "Other",
            "OTKF": "Old Town Keller Foundation",
            "PAC": "Panola College",
            "PADUC": "Bicentennial City County Library",
            "PAHA": "Palacios Area Historical Association",
            "PALACL": "Palacios Library",
            "PANAM": "The University of Texas-Pan American",
            "PAPL": "Port Arthur Public Library",
            "PBPM": "Permian Basin Petroleum Museum, Library and Hall of Fame",
            "PCBARTH": "Private Collection of Marie Bartholomew",
            "PCBG": "Private Collection of Bouncer Goin",
            "PCCM": "Pioneer City County Museum",
            "PCCRD": "Private Collection of Charles R. Delphenis",
            "PCCRS": "Private Collection of Caroline R. Scrivner Richards",
            "PCCW": "Private Collection of Carolyn West",
            "PCEBF": "Private Collection of the Ellis and Blanton Families",
            "PCEV": "Private Collection of Elsa Vorwerk",
            "PCHBF": "Private Collection of the Raymond B. Holbrook Family",
            "PCHBM": "Private Collection of Howard and Brenda McClurkin",
            "PCJB": "Private Collection of Jim Bell",
            "PCJEH": "Private Collection of Joe E. Haynes",
            "PCJM": "Private Collection of Jim McDermott",
            "PCJW": "Private Collection of Judy Wood and Jim Atkinson",
            "PCMB": "Private Collection of Melvin E. Brewer",
            "PCMC": "Private Collection of Mike Cochran",
            "PCMM": "Private Collection of Mary Newton Maxwell",
            "PCMW": "Private Collection of Margay Welch",
            "PCRAS": "Private Collection of Rev. Andrew Stafford",
            "PCSF": "Private Collection of the Sutherlin Family",
            "PCTF": "Private Collection of the Tarver Family",
            "PCTP": "Private Collection of Thadious Polasek",
            "PEMA": "Private Collection of E. M. Adams",
            "PHPL": "Patrick Heath Public Library",
            "PHRML": "Pharr Memorial Library",
            "PJFC": "Price Johnson Family Collection",
            "PLNC": "Private Collection of Lucy Nance Croft",
            "PMNC": "Pearce Museum at Navarro College",
            "POSTPL": "Post Public Library",
            "POTPL": "Pottsboro Public Library",
            "PPCH": "Palo Pinto County Historical Association",
            "PPHM": "Panhandle-Plains Historical Museum",
            "PPL": "Palestine Public Library",
            "PRSD": "Preservation Dallas",
            "PTBW": "Private Collection of T. B. Willis",
            "PVAMU": "Prairie View A&M University",
            "RAINCO": "Rains County Library",
            "REBER": "Reber Memorial Library",
            "REMM": "Reagan County Library",
            "RGPL": "Rio Grande City Public Library",
            "RICE": "Rice University Woodson Research Center",
            "ROCKD": "Lucy Hill Patterson Memorial Library",
            "ROSA": "Private Collection of Rosa Walston Latimer",
            "ROSEL": "Rosenberg Library",
            "RPL": "Richardson Public Library",
            "RRCPL": "Red River County Public Library",
            "RSMT": "Rose Marine Theatre",
            "RVPM": "River Valley Pioneer Museum",
            "SACH": "Sachse Public Library",
            "SACS": "San Antonio Conservation Society",
            "SANJA": "San Jacinto College",
            "SAPL": "San Antonio Public Library",
            "SBL": "Sammy Brown Library",
            "SCHM": "Schulenburg Historical Museum",
            "SCHS": "Smith County Historical Society",
            "SCHU": "Schreiner University",
            "SCL": "Stamford Carnegie Library",
            "SCPL": "Schulenburg Public Library",
            "SDEC": "St. David’s Episcopal Church",
            "SEYM": "Baylor County Free Library",
            "SFAETRC": "Stephen F. Austin East Texas Research Center",
            "SFASF": "Stephen F. Austin Assn. dba Friends of the San Felipe "
                     "State Historic Site",
            "SFMDP": "The Sixth Floor Museum at Dealey Plaza",
            "SGML": "Singletary Memorial Library",
            "SHAMR": "Shamrock Public Library",
            "SHML": "Stella Hill Memorial Library",
            "SILSB": "Silsbee Public Library",
            "SINPL": "Sinton Public Library",
            "SJMH": "San Jacinto Museum of History",
            "SMITHPL": "Smith Public Library",
            "SMLAW": "St. Mary’s University School of Law",
            "SMLBL": "St. Mary's University Louis J. Blume Library",
            "SMU": "Southern Methodist University Libraries",
            "SMVPL": "Smithville Public Library",
            "SOUTHUN": "Southwestern University",
            "SPJST": "Slovanska Podporujici Jednota Statu Texas",
            "SPL": "Salado Public Library",
            "SRH": "Sam Rayburn House Museum",
            "SRPL": "Sanger Public Library",
            "SSPL": "Sulphur Springs Public Library",
            "STAR": "Star of the Republic Museum",
            "STEDU": "St. Edward’s University",
            "STEPH": "Stephenville Public Library",
            "STPC": "St. Philips College",
            "STPRB": "Texas State Preservation Board",
            "STWCL": "Stonewall County Library",
            "STXCL": "South Texas College of Law",
            "SWATER": "Sweetwater/Nolan County City-County Library",
            "SWCL": "Swisher County Library",
            "TADM": "The 12th Armored Division Memorial Museum",
            "TAEA": "Texas Art Education Association",
            "TAMS": "Texas Academy of Mathematics and Science",
            "TAS": "Texas Archeological Society",
            "TCA": "Tarrant County Archives",
            "TCAF": "Texas Chapter of the American Fisheries Society",
            "TCBC": "Texas Coastal Bend Collection",
            "TCC": "Tarrant County College NE, Heritage Room",
            "TCCO": "Travis County Clerk’s Office",
            "TCFA": "Talkington Clement Family Archives",
            "TCHC": "Travis County Historical Commission",
            "TCU": "TCU Mary Couts Burnett Library",
            "TDCD": "Travis County District Clerk’s Office",
            "THC": "Texas Historical Commission",
            "THF": "Texas Historical Foundation",
            "TIMP": "Timpson Public Library",
            "TLPSC": "Tittle-Luther/Parkhill, Smith and Cooper, Inc.",
            "TMCL": "Texas Medical Center Library",
            "TMFM": "Texas Military Forces Museum",
            "TPL": "Taft Public Library",
            "TRHF": "Two Rivers Historical Foundation",
            "TSGS": "Texas State Genealogical Society",
            "TSHA": "Texas State Historical Association",
            "TSLAC": "Texas State Library and Archives Commission",
            "TSOU": "Texas Southern University",
            "TSU": "Tarleton State University",
            "TTRCB": "Texas Tech University Rawls College of Business",
            "TWSU": "Texas Wesleyan University",
            "TWU": "Texas Woman's University",
            "TXDTR": "Texas Department of Transportation",
            "TXLU": "Texas Lutheran University",
            "TXMA": "Texas Medical Association",
            "TXPWD": "Texas Parks & Wildlife Department",
            "TXSU": "Texas State University",
            "TYHL": "Tyrrell Historical Library",
            "TYPL": "Taylor Public Library",
            "UDAL": "University of Dallas",
            "UH": "University of Houston Libraries' Special Collections",
            "UNT": "UNT Libraries",
            "UNTA": "UNT Libraries Special Collections",
            "UNTCAS": "UNT College of Arts and Sciences",
            "UNTCED": "UNT College of Education",
            "UNTCEDR": "UNT Center for Economic Development and Research",
            "UNTCEP": "UNT Center For Environmental Philosophy",
            "UNTCOB": "UNT College of Business",
            "UNTCOE": "UNT College of Engineering",
            "UNTCOI": "UNT College of Information",
            "UNTCOM": "UNT College of Music",
            "UNTCPA": "UNT College of Public Affairs and Community Service",
            "UNTCPHR": "UNT Center for Psychosocial Health Research",
            "UNTCVA": "UNT College of Visual Arts + Design",
            "UNTD": "UNT Dallas",
            "UNTDCL": "UNT Dallas College of Law",
            "UNTDP": "UNT Libraries Digital Projects Unit",
            "UNTG": "University of North Texas Galleries",
            "UNTGD": "UNT Libraries Government Documents Department",
            "UNTGSJ": "UNT Frank W. and Sue Mayborn School of Journalism",
            "UNTHON": "UNT Honors College",
            "UNTHSC": "UNT Health Science Center",
            "UNTIAS": "UNT Institute of Applied Sciences",
            "UNTLML": "UNT Media Library",
            "UNTLTC": "UNT Linguistics and Technical Communication Department",
            "UNTML": "UNT Music Library",
            "UNTOHP": "UNT Oral History Program",
            "UNTOS": "UNT Office of Sustainability",
            "UNTP": "UNT Press",
            "UNTRB": "UNT Libraries Rare Book and Texana Collections",
            "UNTSMHM": "UNT College of Merchandising, Hospitality and Tourism",
            "UNTX": "University of North Texas",
            "URCM": "University Relations, Communications & Marketing  "
                    "department for UNT",
            "UT": "University of Texas",
            "UTA": "University of Texas at Arlington Library",
            "UTD": "The University of Texas at Dallas",
            "UTEP": "University of Texas at El Paso",
            "UTHSC": "University of Texas Health Science Center Libraries",
            "UTMDAC": "University of  Texas MD Anderson Center",
            "UTPB": "University of Texas Permian Basin",
            "UTSA": "UT San Antonio Libraries Special Collections",
            "UTSW": "UT Southwestern Medical Center Library",
            "VCHC": "Val Verde County Historical Commission",
            "VCUH": "Victoria College/University of Houston-Victoria Library",
            "VZCL": "Van Zandt County Library",
            "WASP": "National WASP WWII Museum",
            "WCGS": "Walker County Genealogical Society",
            "WCHM": "Wolf Creek Heritage Museum",
            "WCHS": "Wilson County Historical Society",
            "WEAC": "Weatherford College",
            "WEBM": "Weslaco Museum",
            "WESTPL": "West Public Library",
            "WHAR": "Wharton County Library",
            "WILLM": "The Williamson Museum",
            "WINK": "Winkler County Library",
            "WNPL": "Winnsboro Public Library",
            "WTM": "Witte Museum",
            "WTXC": "Western Texas College Library",
            "WYU": "Wiley College",
            "ZULA": "Zula B. Wylie Memorial Library"
        }

    def map_date(self):
        """
        Use the first instance of untl:date where the qualifier == "creation"
        otherwhise use the first instance of untl:date where
        qualifier != "digitized" or qualifier != "embargoUntil"
        """

        prop = self.root_key + "date"

        if exists(self.provider_data, prop):
            date = None

            creation_date = None
            non_digitized_embargo_until_date = None
            for s in iterify(getprop(self.provider_data, prop)):
                if "qualifier" in s:
                    if s["qualifier"] == "creation":
                        creation_date = s.get("#text")
                        break
                    elif (s["qualifier"] not in
                          ["digitized", "embargoUntil"] and \
                          non_digitized_embargo_until_date is None):
                        non_digitized_embargo_until_date = s.get("#text")

            if creation_date is not None:
                date = creation_date
            else:
                date = non_digitized_embargo_until_date

            if date:
                self.update_source_resource({"date": date})

    def map_title(self):
        prop = self.root_key + "title"

        if exists(self.provider_data, prop):
            title = []
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, basestring):
                    title.append(s)
                elif "#text" in s:
                    title.append(s["#text"])

            if title:
                self.update_source_resource({"title": title})

    def map_rights(self):
        prop = self.root_key + "rights"

        if exists(self.provider_data, prop):
            rights = None
            license = None
            statement = None
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    qualifier = s.get("qualifier")
                    text = s.get("#text")
                except:
                    continue

                if qualifier == "license":
                    try:
                        license = "License: " + self.rights_term_label[text]
                    except:
                        msg = ("Term %s not in self.rights_term_label for %s" %
                               (text, self.provider_data["_id"]))
                        logger.error(msg)
                elif qualifier == "statement":
                    statement = text

            rights = "; ".join(filter(None, [rights, statement]))

            if rights:
                self.update_source_resource({"rights": rights})

    def map_type(self):
        prop = self.root_key + "format"

        if exists(self.provider_data, prop):
            format_to_type = {
                "audio": "sound",
                "video": "moving image",
                "website": "interactive resource"
            }

            _type = []
            for s in iterify(getprop(self.provider_data, prop)):
                if s == "other":
                    pass
                elif s in format_to_type.keys():
                    _type.append(format_to_type[s])
                else:
                    _type.append(s)

            if _type:
                self.update_source_resource({"type": _type})

    def map_creator(self):
        prop = self.root_key + "creator"

        if exists(self.provider_data, prop):
            creator = []
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, basestring):
                    creator.append(s)
                elif "name" in s:
                    creator.append(s["name"])

            if creator:
                self.update_source_resource({"creator": creator})

    def map_subject(self):
        prop = self.root_key + "subject"

        if exists(self.provider_data, prop):
            subject = []
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, basestring):
                    subject.append(s)
                elif "#text" in s:
                    subject.append(s["#text"])

            if subject:
                self.update_source_resource({"subject": " - ".join(subject)})

    def map_relation(self):
        prop = self.root_key + "relation"

        if exists(self.provider_data, prop):
            relation = []
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, basestring):
                    relation.append(s)
                elif "#text" in s:
                    relation.append(s["#text"])

            if relation:
                self.update_source_resource({"relation": relation})

    def map_spatial(self):
        prop = self.root_key + "coverage"

        if exists(self.provider_data, prop):
            spatial = []
            qualifiers = ["placeName", "placePoint", "placeBox"]
            for s in iterify(getprop(self.provider_data, prop)):
                if "qualifier" in s and s["qualifier"] in qualifiers:
                    spatial.append(s.get("#text"))
            spatial = filter(None, spatial)

            if spatial:
                self.update_source_resource({"spatial": spatial})

    def map_publisher(self):
        prop = self.root_key + "publisher"

        if exists(self.provider_data, prop):
            publisher = []
            for s in iterify(getprop(self.provider_data, prop)):
                if "location" in s and "name" in s:
                    publisher.append("%s: %s" % (s["location"].strip(),
                                                 s["name"].strip()))

            if publisher:
                self.update_source_resource({"publisher": publisher})

    def map_contributor(self):
        prop = self.root_key + "contributor"

        if exists(self.provider_data, prop):
            contributor = []
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, basestring):
                    contributor.append(s)
                elif "name" in s:
                    contributor.append(s["name"])

            if contributor:
                self.update_source_resource({"contributor": contributor})
            else:
                logger.error("No contributor for record %s" %
                             self.provider_data["_id"])

    def map_description(self):
        prop = self.root_key + "description"

        if exists(self.provider_data, prop):
            description = []
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, basestring):
                    description.append(s)
                elif "#text" in s:
                    description.append(s["#text"])

            if description:
                self.update_source_resource({"description": description})

    def map_language(self):
        prop = self.root_key + "language"

        if exists(self.provider_data, prop):
            self.update_source_resource({"language":
                                         getprop(self.provider_data, prop)})

    def map_data_provider(self):
        prop = "originalRecord/header/setSpec"

        if exists(self.provider_data, prop):
            dataprovider = []
            for s in iterify(getprop(self.provider_data, prop)):
                if "partner" in s:
                    term = s.split(":")[-1]
                    try:
                        dataprovider.append(self.dataprovider_term_label[term])
                    except:
                        logger.debug("Term %s does not exist in " % term +
                                     "self.dataprovider_term_label for %s" %
                                     self.provider_data["_id"])

            if dataprovider:
                self.mapped_data.update({"dataProvider": dataprovider})

    def map_spec_type_and_format(self):
        prop = self.root_key + "resourceType"

        if exists(self.provider_data, prop):
            _dict = {}
            spec_type = None
            for s in iterify(getprop(self.provider_data, prop)):
                values = s.split("_")
                _dict["format"] = values[0]
                try:
                    if values[1] in ["book", "newspaper", "journal"]:
                        spec_type = values[1]
                    elif values[1] == "leg":
                        spec_type = "government document"
                    elif values[1] == "serial":
                        spec_type = "journal"
                except:
                    pass
            if spec_type is not None:
                _dict["specType"] = spec_type.title()

            self.update_source_resource(_dict)

    def map_identifier_object_and_is_shown_at(self):
        prop = self.root_key + "identifier"

        if exists(self.provider_data, prop):
            _dict = {}
            identifier = []
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    qualifier = s.get("qualifier")
                    text = s.get("#text")
                    if qualifier and text:
                        identifier.append("%s: %s" % (qualifier, text))

                    if qualifier == "itemURL":
                        _dict["isShownAt"] = text
                    elif qualifier == "thumbnailURL":
                        _dict["object"] = text
                except:
                    pass

            if identifier:
                self.update_source_resource({"identifier": identifier})

            self.mapped_data.update(_dict)

    def map_multiple_fields(self):
        self.map_spec_type_and_format()
        self.map_identifier_object_and_is_shown_at()
