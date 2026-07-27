// CitadelEditorModule.cpp — the CitadelEditor module entry point.
//
// Registers a "Citadel" submenu under the Level Editor's Tools menu with a single
// "Cook Map Data" action that drives FCitadelMapCooker. The menu is registered via
// UToolMenus (UE5's data-driven menu system); registration is deferred to a
// startup callback because UToolMenus is not guaranteed ready at module-load time.
#include "CitadelMapCooker.h"

#include "Modules/ModuleManager.h"
#include "ToolMenus.h"

#define LOCTEXT_NAMESPACE "CitadelEditor"

class FCitadelEditorModule : public IModuleInterface
{
public:
	virtual void StartupModule() override
	{
		// UToolMenus may not exist yet at load time; register once it does.
		UToolMenus::RegisterStartupCallback(FSimpleMulticastDelegate::FDelegate::CreateRaw(
			this, &FCitadelEditorModule::RegisterMenus));
	}

	virtual void ShutdownModule() override
	{
		UToolMenus::UnRegisterStartupCallback(this);
		UToolMenus::UnregisterOwner(this);
	}

private:
	void RegisterMenus()
	{
		FToolMenuOwnerScoped OwnerScoped(this);

		UToolMenu* ToolsMenu = UToolMenus::Get()->ExtendMenu("LevelEditor.MainMenu.Tools");
		if (!ToolsMenu)
		{
			return;
		}
		FToolMenuSection& Section =
			ToolsMenu->FindOrAddSection("Citadel", LOCTEXT("CitadelSection", "Citadel"));

		Section.AddSubMenu(
			"Citadel",
			LOCTEXT("CitadelSubMenu", "Citadel"),
			LOCTEXT("CitadelSubMenuTip", "Citadel authoring tools"),
			FNewToolMenuDelegate::CreateRaw(this, &FCitadelEditorModule::BuildCitadelSubMenu));
	}

	void BuildCitadelSubMenu(UToolMenu* SubMenu)
	{
		FToolMenuSection& Section =
			SubMenu->FindOrAddSection("Map", LOCTEXT("CitadelMapSection", "Map"));
		Section.AddMenuEntry(
			"CookMapData",
			LOCTEXT("CookMapData", "Cook Map Data"),
			LOCTEXT("CookMapDataTip",
			        "Export the current level's static collision geometry to a Citadel .map file."),
			FSlateIcon(),
			FUIAction(FExecuteAction::CreateStatic(&FCitadelMapCooker::CookCurrentLevelInteractive)));
	}
};

#undef LOCTEXT_NAMESPACE

IMPLEMENT_MODULE(FCitadelEditorModule, CitadelEditor);
