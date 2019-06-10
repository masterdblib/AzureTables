
git push -f


first Download the latest paket.bootstrapper.exe 'https://github.com/fsprojects/Paket/releases/latest' into that directory.
=> run it to update paket exe
=> run ".paket/paket.exe" install 
This will fetch nuget.exe from nuget.org and also download an early version of NUnit that contains 
the NUnit runner. The edit to paket.dependencies does not replace the RestorePackages() step. 
The NUnit.MasterDbLib.AzureTables.Tests test project references the NUnit version 2.6.2 library, 
so we need that version too.


=============================================================

TO ADD NUGET FROM PRIVATE FEED DO

".paket//paket.exe" install --redirects --clean-redirects


".paket//paket.exe"  convert-from-nuget -v -f
 
".paket//paket.exe" clear-cache -v

".paket//paket.exe" simplify



step 1 download the CredentialProvider

step 2: get username and passwordt

>CredentialProvider.VSS.exe -U https://pkgs.dev.azure.com/CalculatorCompanyDev/_packaging/MasterDbLib.AzureTables/nuget/v3/index.json
Getting new credentials for source:https://pkgs.dev.azure.com/CalculatorCompanyDev/_packaging/MasterDbLib.AzureTables/nuget/v3/index.json, scope:vso.packaging_write vso.drop_write
{"__VssPasswordWarning":"WARNING: Treat the authentication token in the passwordt.","Username":"VssSessionToken","Password":"eyJ0eXAiOiJtWhwA"}


setp 3 run
".paket/paket.exe" config add-credentials https://pkgs.dev.azure.com/CalculatorCompanyDev/_packaging/MasterDbLib.AzureTables/nuget/v3/index.json  --verify

step 4 :

".paket//paket.exe" install --redirects --clean-redirects