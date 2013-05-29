# mETL

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
   - **BaseExpander**: Kiterjesztésre használható osztály, elsődleges feladata olyan esetben van, ahol egy rekordot többszöröznénk meg.

### Komponens ábra
<img src="docs/components.png" alt="Folyamat" style="width: 100%;"/>

## Telepítés
Hagyományos Python csomagként a telepítést legegyszerűbben a következő parancs kiadásával tehetjük meg a mETL könyvtárában állva:
`python setup.py install`

Csomagot ezt követően az alábbi paranccsal tesztelhetjük:
`python setup.py test`

Következő függőségekkel rendelkezik: xlrd, gdata, demjson, pyyaml, sqlalchemy, xlwt, tarr, nltk, xlutils

### Mac OSX
Telepítés előtt a következő csomagok feltételére lesz szükség, mely a következő:

- XCode telepítése
- XCode "Command Line Tools" telepítése
- [Macports telepítése](https://distfiles.macports.org/MacPorts/MacPorts-2.1.3-10.8-MountainLion.pkg)

Ezt követően minden csomag megfelelően feltelepítésre kerül.

### Linux
Telepítés előtt a `python-setuptools` meglétét ellenőrizni kell, illetve hiányzása esetén `apt-get install`-al telepíteni.

### Windows
Csomagok nagy része probléma nélkül feltelepül a számítógépre, azonban a folyamat előtt pár csomag meglétéről manuálisan kell gondoskodni.

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
