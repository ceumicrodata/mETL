
# Change Log

### Version 0.1.6
- .0: Changed XML converter to <a href="https://github.com/bfaludi/XML2Dict">xml2dict</a> package. 

   **IMPORTANT**: It has a new XML mapping technique, all XML source map must be updated!
   
   * For element's value: path/for/xml/element/**text**
   * For element's attribute: path/for/xml/element/attributename

- .0: Fixed a bug in XML sources when multiple list element founded at sub-sub-sub level.
- .0: Fixed a bug with htaccess file opening in CSV, TSV, Yaml, JSON sources.
- .1: Fixed a bug where map ending was *
- .1: Added SetWithMap modifier and Complex type

### Version 0.1.5
- .0: htaccess file opening support.
- .1: List type JSON support for database target and source.
- .1: ListExpander with map ability.

### Version 0.1.4
- .0: First public release.
- .1: Remove elementtree and cElementTree dependencies.
- .2: TARR dependency link added, PyXML dependency removed.
- .3: JSON target get a compact format parameter to create pretty printed files.
- .4: Update TARR dependency.
- .5: Add missing dependency: python-dateutil
- .6: Fixed xml test case after 2.7.2 python version.
- .7: Fixed List type converter for string or unicode data. It will not split the string!
- .8: Fixed JSON source when no root iterator given and the resource file is contains only one dictionary.
- .9: Added a new operator `!` to convert dictionart into list in mapping process.
- .10: Fixed a bug in Windows when want to open a resource with absolute path.
- .11: Added ListExpander to expand list information into single fields.
- .12: XML source open via http and https protocols.

# Documentation

## Alapok
Az mETL egy olyan **ETL eszköz**, amely kifejezetten a CEU számára szükséges választási adatok adatok betöltésére jött létre. Természetesen a program ennél sokkal általánosabb körű, gyakorlatilag bármilyen adat betöltésre alkalmazható. A program fejlesztése **Python**-ban történt, az optimális memóriahasználat maximális figyelembevételével a **Brewery** eszköz képességeinek felmérést követően.

## Képességek
Leggyakoribb fájlformátumokra biztosít a program aktuális verziója támogatást **migrációk és migrációs csomagok** kezelésével. Ezek a következőek típusonként.

**Forrás típusok**:

- CSV, TSV, XLS, Google SpreadSheet, Fix szélességű fájl
- PostgreSQL, MySQL, Oracle, SQLite, Microsoft SQL Server
- JSON, XML, YAML

**Cél típusok:**

- CSV, TSV, XLS - fájl folytatással is
- Fix szélességű fájl
- PostgreSQL, MySQL, Oracle, SQLite, Microsoft SQL Server - módosítás céllal is
- JSON, XML, YAML

Fejlesztés folyamán igyekeztünk a leggyakoribb transzformációs lépésekkel, programszerkezetekkel, és manipulációs lépésekkel is ellátni a teljes feldolgozási folyamatot. Ennek fényében a program a következő transzformációkkal rendelkezik alapértelmezetten:

- **Add**: Hozzáad egy tetszőleges számot egy értékhez.
- **Clean**: Eltávolítja a különféle írásjeleket. (pont, vessző, stb.)
- **ConvertType**: Módosítja a mező típusát egy másik típusra.
- **Homogenize**: Az ékezetes karaktereket ékezet nélküliekre alakítja. (NFKD formátum)
- **LowerCase**: Kisbetűssé alakítás.
- **Map**: Kicserél mező értékeket, más értékre.
- **RemoveWordsBySource**: Egy másik forrás állományt felhasználva eltávolít szavakat.
- **ReplaceByRegexp**: Reguláris kifejezés alapján cserét hajt végre.
- **ReplaceWordsBySource**: Egy másik forrás állományt felhasználva lecserél szavakat.
- **Set**: Érték beállítást végez.
- **Split**: Szóközök mentén elválasztja a szavakat és a megadott intervallumot hagyja meg.
- **Stem**: Szótőre hozás.
- **Strip**: Eltávolítja az érték elején és végén levő felesleges szóközöket vagy egyéb karaktereket.
- **Sub**: Kivon egy számot egy értékből.
- **Title**: Minden szót nagy kezdőbetűssé alakít.
- **UpperCase**: Nagybetűssé alakítás.

Manupulációk esetében három csoportot különböztetünk meg:

1. **Modifier**

   Módosítók azok az objektumok, amelyek egy teljes sort (rekordot) kapnak, és mindig egy teljes sorral térnek vissza. Azonban a folyamataik során érték módosításokat végeznek a különböző mezők összefüggő értékeinek felhasználásával.
   
   - **Set**: Érték beállítást végez fix érték séma, függvény, vagy másik forrás felhasználásával.
   - **TransformField**: Hagyományos mező szintű transzformáció hívható általa a manipulációs lépés során. 
   
2. **Filter**

   Szűrést végeznek elsősorban. Olyankor használatosak, amikor a korábbi lépésekben transzformációk segítségével megtisztított értékeket szeretnénk kiértékelni és eldobni, ha a rekordot hiányosnak, vagy nem megfelelőnek ítéljük meg.

   - **DropByCondition**: Feltétel alapján dönthető el a rekord sorsa.
   - **DropBySource**: Másik forrás állományban történő szereplés dönt a rekord sorsáról.
   - **DropField**: Rekord számot ugyan nem csökkent, de mezők törölhetőek a segítségével.

3. **Expand**

   Bővítésre használjuk, ha további értékeket szeretnénk a jelenlegi forrás után helyezni.

   - **Append**: Mostanival teljesen megegyező forrásállomány beszúrása a folyamatba az aktuális után.
   - **AppendBySource**: Másik forrás állomány tartalma szúrható az eredeti forrás után.
   - **Field**: Paraméterül megadott oszlopokat egy másik oszlopba gyűjt össze az oszlop értékeivel.
   - **BaseExpander**: Kiterjesztésre használható osztály, elsődleges feladata olyan esetben van, ahol egy rekordot többszöröznénk meg.
   - **ListExpander**: Lista típusú elemet bont értékei alapján külön sorokba.
   - **Melt**: Megadott oszlopokat rögzíti és a többi oszlopot kulcs-érték párok alapján jeleníti meg. 

### Komponens ábra
<img src="docs/components.png" alt="Folyamat" style="width: 100%;"/>

## Telepítés
Hagyományos Python csomagként a telepítést legegyszerűbben a következő parancs kiadásával tehetjük meg a mETL könyvtárában állva:
`python setup.py install`

Csomagot ezt követően az alábbi paranccsal tesztelhetjük:
`python setup.py test`

Következő függőségekkel rendelkezik: xlrd, gdata, demjson, pyyaml, sqlalchemy, xlwt, tarr, nltk, xlutils, xml2dict

### Mac OSX
Telepítés előtt a következő csomagok feltételére lesz szükség, mely a következő:

- XCode telepítése
- XCode "Command Line Tools" telepítése
- [Macports telepítése](https://distfiles.macports.org/MacPorts/MacPorts-2.1.3-10.8-MountainLion.pkg)

Ezt követően minden csomag megfelelően feltelepítésre kerül.

### Linux
Telepítés előtt a `python-setuptools` meglétét ellenőrizni kell, illetve hiányzása esetén `apt-get install`-al telepíteni.

### Windows
Minden csomag könnyedén feltelepíthető!

## Futtatás
Konzol scriptek gyűjteménye a program, amely emiatt bármilyen rendszerbe könnyen beépíthető, és akár cron script-ek segítségével időzíthető is. 

**Következő script-ekből áll a program:**

1. `metl [options] CONFIG.YML`

   Egy teljes folyamat indítható el a segítségével a paraméterül kapott YAML fájl alapján. A konfigurációban megadott folyamatokat a konfigurációs állománynak teljesen le kell írnia, input és output fájlok pontos útvonalával együtt.
   
   - `-t`: Futtatás során migrációs állomány készítése az aktuális adatok állapotából.
   - `-m`: Korábbi migrációs állomány átadása, amely az előző futtatott verzióé volt.
   - `-p`: Mappa átadása, amit hozzá adunk a PATH változóhoz, hogy a YAML konfigurációban történő hivatkozás megvalósulhasson külső python állományra.
   - `-d`: Debug mód, mindent kiír a stdout-ra.
   - `-l`: Hány elemen történjen a feldolgozás. Nagyszerű lehetőség nagy fájlok tesztelésére kis rekordokon, amíg minden nem úgy működik, ahogy szeretnénk.
   - `-o`: Hányadik elemtől kezdje a feldolgozást.
   - `-s`: Ha a konfigurációs állomány nem tartalmazza a resource útvonalát, itt is megadható.

   Migrációról és a `-p` kapcsolóról később lesz szó részletesebben.

2. `metl-walk [options] BASECONFIG.YML FOLDER`
   
   Feladata a paraméterül kapott YAML fájl alkalmazása, minden paraméterül kapott mappában szereplő állományra nézve. A konfigurációnak ebben az esetben nem kell az input fájlok elérhetőségét tartalmazni, a script automatikusan elvégzi ezek behelyettesítését.
   
   - `-m`: Multiprocessing bekapcsolása több CPU-val rendelkező gépeken. A feldolgozandó állományok külön thread-ekbe kerülnek. Használata **csak és kizárólag** `Database` cél esetén szabad, mindenhol máshol problémákat okoz!
   - `-p`: Mappa átadása, amit hozzá adunk a PATH változóhoz, hogy a YAML konfigurációban történő hivatkozás megvalósulhasson külső python állományra.
   - `-d`: Debug mód, mindent kiír a stdout-ra.
   - `-l`: Hány elemen történjen a feldolgozás. Nagyszerű lehetőség nagy fájlok tesztelésére kis rekordokon, amíg minden nem úgy működik, ahogy szeretnénk.
   - `-o`: Hányadik elemtől kezdje a feldolgozást.
   
   A `-p` kapcsolóról később lesz szó részletesebben.
   
3. `metl-transform [options] CONFIG.YML FIELD VALUE`
   
   Feladata a YAML fájlban szereplő egyik mező transzformációs lépéseinek tesztelése. Paraméterül várja a mező megnevezését, és azt az értéket, amelyen a tesztelést végeznénk. A script ki fogja írni lépésről-lépésre a mező értékének alakulását.

   - `-p`: Mappa átadása, amit hozzá adunk a PATH változóhoz, hogy a YAML konfigurációban történő hivatkozás megvalósulhasson külső python állományra.
   - `-d`: Debug mód, mindent kiír a stdout-ra.
   
   A `-p` kapcsolóról később lesz szó részletesebben.
   
4. `metl-aggregate [options] CONFIG.YML FIELD`

   Feladata kigyűjteni a paraméterül átadott mező összes lehetséges értékét. Ezen értékek alapján utána már könnyen készíthető Map a rekordokhoz.

   - `-p`: Mappa átadása, amit hozzá adunk a PATH változóhoz, hogy a YAML konfigurációban történő hivatkozás megvalósulhasson külső python állományra.
   - `-d`: Debug mód, mindent kiír a stdout-ra.
   - `-l`: Hány elemen történjen a feldolgozás. Nagyszerű lehetőség nagy fájlok tesztelésére kis rekordokon, amíg minden nem úgy működik, ahogy szeretnénk.
   - `-o`: Hányadik elemtől kezdje a feldolgozást.
   - `-s`: Ha a konfigurációs állomány nem tartalmazza a resource útvonalát, itt is megadható.
   
   A `-p` kapcsolóról később lesz szó részletesebben.
   
5. `metl-differences [options] CURRENT_MIGRATION LAST_MIGRATION`

   Feladata két különböző migráció összehasonlítása. Első paramétere a friss, és második paramétere a korábbi migráció. A script megmondja, mennyi elem került bele az újba, mennyi elem módosult, mennyi elem maradt változatlan, illetve került törlésre.
   
   - `-n`: Konfigurációs állomány az új elemek kulcsainak kiírására.
   - `-m`: Konfigurációs állomány a módosult elemek kulcsainak kiírására.
   - `-u`: Konfigurációs állomány a módosulatlan elemek kulcsainak kiírására.
   - `-d`: Konfigurációs állomány a törölt elemek kulcsainak kiírására.
 
## Működés

Az eszköz egy **YAML fájlt használ konfigurációnak**, ami leírja a teljes végrehajtás útját, és az összes elvégzendő transzformációs lépést.

**Rövid működése egy átlagos programnak a következőképpen néz ki:**

1. A program beolvassa a megadott forrás állományt.
2. Soronként egy illesztés felhasználásával betölti megfelelő mezőkbe a sor értékeit.
3. Mezőkön egyesével hívódnak meg a tetszőleges bonyolultságú transzformációk.
4. Végleges, transzformációkon átjutott sor kerül az első manipulációhoz, ahol a további szűrések, módosítások már a teljes sor értékeire érvényben lehetnek. Minden manipuláció a következő manipulációs lépés számára adja át a már konvertált és feldolgozott sort.
5. Cél típushoz kerülés után, megtörténik a végleges sor kiíársa a megadott típusú állományba.

Nézzük meg a működés során használt összes komponenset részletesen, majd pedig nézzük meg a felsorolt lépésekből, hogyan tudunk konfigurációs YAML állományokat készíteni.

Az alábbi dokumentáció két témakört igyekszik lefedni, egyrészt definiálja, hogy a YAML konfigurációban hogyan tudjuk leírni a szükséges feladatokat, illetve egy rövid betekintést ad példákon keresztül a Python oldali kódba és legfontosabb metódusokba, hogy ha az alap eszköz funkció készlete kevésnek bizonyulna, hogyan tudunk gyorsan és egyszerűen kiegészítő feltételeket, módosítókat készíteni.

### Működési ábra
<img src="docs/workflow.png" alt="Folyamat" style="width: 100%;"/>

