// CitadelClientModule.cpp — the module implementation entry point.
//
// Every Unreal module DLL needs exactly ONE IMPLEMENT_MODULE so the engine's
// FModuleManager can find and initialize it when the DLL is loaded. Without it
// the DLL still compiles and links (the compile-verify passes), but at load time
// the editor reports "module 'CitadelClient' could not be initialized
// successfully after it was loaded" and the plugin fails to load.
//
// CitadelClient needs no custom startup/shutdown work (the gameplay lives in the
// subsystem + components), so the engine's default module implementation is used.
#include "Modules/ModuleManager.h"

IMPLEMENT_MODULE(FDefaultModuleImpl, CitadelClient);
