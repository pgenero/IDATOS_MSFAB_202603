[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "humanresources department_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"humanresources department" = let
  Origen = PostgreSQL.Database("wcalcagno.postgres.database.azure.com", "adventureworks"),
  #"Navegación 1" = Origen{[Schema = "humanresources", Item = "department"]}[Data],
  #"Se han quitado otras columnas." = Table.SelectColumns(#"Navegación 1", {"name", "groupname", "departmentid"})
in
  #"Se han quitado otras columnas.";
shared #"humanresources department_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "1867f4b3-ee9d-43a9-90ed-724dca17ead9"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "9a34477d-cffc-4f92-86a9-b539e39521f7"]}[Data],
  TableNavigation = Navigation_2{[Id = "humanresources department", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
