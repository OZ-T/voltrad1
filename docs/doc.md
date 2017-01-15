# use Pandoc’s Markdown: http://www.johnmacfarlane.net/pandoc/
# or use reStructuredText markup: http://docutils.sourceforge.net/rst.html

Configuration

The specific paths are configured in a configuration file residing in the user home folder called .voltrad1

this file should be copied to home user folder upon package installation
if that file doesn't exist means that we are in the developement environment so the file inside the
project folder should be used instead (voltrad1.ini)

Folders:

    bin
        ficheros ejecutables, por definir
    config
        contiene ficheros csv con el listado de subyacentes del universo de inversión
    db
        almacenamiento en ficheros hdf5
    docs
        documentación del proyecto
    tests
        unit testing

    volaccounting
        contabilidad básica del cuenta en IB
    volbacktest
        backtesting de estretegias con opciones
    voldailyanalytics

    volibutils
        contiene los desarrollos para integrarse con el API de IB. Hay pruebas con el paquete IBPy
        pero finalmente la integración se hace usando swigibpy.
        La clase cliente es sync_client.py que es la que incluye la implementación principal con el
        cliente IB y el wrapper con los callbacks para recibir la informacion.
    volmktscanner
    volquotes
    volreporting
    volsetup
        incluye un config.py con métodos utiles para acceder a la configuración del entorno
        ficheros ini, csv, paths etc.
    volstrateg

    voltrad.ini
        fichero ini de configuración de los programas. Incluye paths dependientes del entorno
        opciones de ejecución y otros valores configurables
        En el caso de windows hay que copiar al home del usuario voltrad.ini
        En el caso de ubuntu hay que copiar al home del usuario renombrandolo a .voltrad

Operations

the file config/commandlist.yaml contains the commands to operate the system. The following is a description
of the workflow for operations with TIC:

TREND ANALYSIS
1.- Check daily Coopock for SPX with command: . p coppock
        Check what coppock has to say about the market trend: positive&down, negative&up, ...
2.- Check Daily summary (YTD, MTD, WTD, DD) for SPX: . p summary "SPX"

RISK ANALYSIS
1.- Check EMAS and IV Channels for SPX: . p emas "SPX"
        Check if there are alerts activated for RSK_EMA50 or CANAL_IV_WK or CANAL_IV_MO
        price tend to be outside these channel a "short" time
2.- Check volatility levels: . p vol "SPX"
        Check if the level of IV (VIX) is inside or outside of bollinger bands (1sd and 2sd)
        so IV will be classified as high, low, extreme high or extreme low
3.- Check rapid movement indicator: . p fastmove "SPX"        