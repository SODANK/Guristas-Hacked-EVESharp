using System;
using System.Collections.Generic;
using System.Linq;
using EVESharp.Database.Account;
using EVESharp.Database.Characters;
using EVESharp.Database.Inventory;
using EVESharp.Database.Inventory.Attributes;
using EVESharp.Database.Inventory.Categories;
using Attribute = EVESharp.Database.Inventory.Attributes.Attribute;
using EVESharp.Database.Inventory.Types;
using EVESharp.Database.Market;
using EVESharp.Database.Old;
using EVESharp.Database.Standings;
using EVESharp.Destiny;
using EVESharp.EVE.Data.Inventory;
using EVESharp.EVE.Data.Inventory.Items;
using EVESharp.EVE.Data.Inventory.Items.Types;
using EVESharp.EVE.Dogma;
using EVESharp.EVE.Exceptions.slash;
using EVESharp.EVE.Market;
using EVESharp.EVE.Network.Services;
using EVESharp.EVE.Network.Services.Validators;
using EVESharp.EVE.Notifications;
using EVESharp.EVE.Notifications.Inventory;
using EVESharp.EVE.Notifications.Skills;
using EVESharp.EVE.Relationships;
using EVESharp.EVE.Sessions;
using EVESharp.Node.Services.Space;
using EVESharp.Types;
using EVESharp.Types.Collections;
using Serilog;
using Type = EVESharp.Database.Inventory.Types.Type;

namespace EVESharp.Node.Services.Network;

[MustBeCharacter]
[MustHaveRole (Roles.ROLE_ADMIN)]
public class slash : Service
{
    private readonly Dictionary <string, Action <string [], ServiceCall>> mCommands =
        new Dictionary <string, Action <string [], ServiceCall>> ();
    public override AccessLevel         AccessLevel           => AccessLevel.None;
    private         ITypes              Types                 => this.Items.Types;
    private         IItems              Items                 { get; }
    private         ILogger             Log                   { get; }
    private         OldCharacterDB      CharacterDB           { get; }
    private         SkillDB             SkillDB               { get; }
    private         INotificationSender Notifications         { get; }
    private         IWallets            Wallets               { get; }
    private         IDogmaNotifications DogmaNotifications    { get; }
    private         IDogmaItems         DogmaItems            { get; }
    private         ISessionManager     SessionManager        { get; }
    private         SolarSystemDestinyManager SolarSystemDestinyMgr { get; }
    private         IStandings          Standings             { get; }

public slash
(
    ILogger             logger,
    IItems              items,
    OldCharacterDB      characterDB,
    INotificationSender notificationSender,
    IWallets            wallets,
    IDogmaNotifications dogmaNotifications,
    IDogmaItems         dogmaItems,
    SkillDB             skillDB,
    ISessionManager     sessionManager,
    SolarSystemDestinyManager solarSystemDestinyMgr,
    IStandings          standings
)
{
    Log                          = logger;
    this.Items                   = items;
    CharacterDB                  = characterDB;
    Notifications                = notificationSender;
    this.Wallets                 = wallets;
    this.DogmaNotifications      = dogmaNotifications;
    this.DogmaItems              = dogmaItems;
    this.SkillDB                 = skillDB;
    this.SessionManager          = sessionManager;
    this.SolarSystemDestinyMgr   = solarSystemDestinyMgr;
    this.Standings               = standings;

    // register commands
    this.mCommands["create"]        = this.CreateCmd;
    this.mCommands["createitem"]    = this.CreateCmd;
    this.mCommands["giveskills"]    = this.GiveSkillCmd;
    this.mCommands["giveskill"]     = this.GiveSkillCmd;
    this.mCommands["giveisk"]       = this.GiveIskCmd;
    this.mCommands["move"]          = this.MoveCmd;
    this.mCommands["heal"]          = this.HealCmd;
    this.mCommands["unload"]        = this.UnloadCmd;
    this.mCommands["spawn"]         = this.SpawnCmd;
    this.mCommands["fit"]           = this.FitCmd;
    this.mCommands["online"]        = this.OnlineCmd;
    this.mCommands["tr"]            = this.TrCmd;
    this.mCommands["removeskill"]   = this.RemoveSkillCmd;
    this.mCommands["removeskills"]  = this.RemoveSkillCmd;
    this.mCommands["spawnn"]        = this.SpawnNCmd;
    this.mCommands["entity"]        = this.EntityCmd;
    this.mCommands["bp"]            = this.BpCmd;
    this.mCommands["load"]          = this.LoadCmd;
    this.mCommands["repairmodules"] = this.RepairModulesCmd;
    this.mCommands["setstanding"]   = this.SetStandingCmd;
    this.mCommands["unspawn"]       = this.UnspawnCmd;
    this.mCommands["moveme"]        = this.MoveMeCmd;
}


    private string GetCommandListForClient ()
    {
        string result = "";

        foreach ((string name, _) in this.mCommands)
            result += $"'{name}',";

        return $"[{result}]";
    }

    public PyDataType SlashCmd (ServiceCall call, PyString line)
    {
        try
        {
            string [] parts = line.Value.Split (' ');

            // get the command name
            string command = parts [0].TrimStart ('/');

            // only a "/" means the client is requesting the list of commands available
            if (command.Length == 0 || this.mCommands.ContainsKey (command) == false)
                throw new SlashError ("Commands: " + this.GetCommandListForClient ());

            this.mCommands [command].Invoke (parts, call);
        }
        catch (SlashError)
        {
            throw;
        }
        catch (Exception e)
        {
            Log.Error (e.Message);
            Log.Error (e.StackTrace);

            throw new SlashError ($"Runtime error: {e.Message}");
        }

        return null;
    }

    // =====================================================================
    //  UTILITY METHODS
    // =====================================================================

    /// <summary>
    /// Resolves "me" to the caller's shipID, or parses argument as an integer itemID.
    /// </summary>
    private int ResolveItemTarget (string arg, ServiceCall call)
    {
        if (arg == "me")
        {
            int? shipID = call.Session.ShipID;

            if (shipID == null || shipID.Value == 0)
                throw new SlashError ("You don't have an active ship");

            return shipID.Value;
        }

        if (int.TryParse (arg, out int itemID))
            return itemID;

        throw new SlashError ($"Invalid target: {arg}");
    }

    /// <summary>
    /// Resolves "me" to the caller's characterID, or parses argument as an integer characterID.
    /// </summary>
    private int ResolveCharacterTarget (string arg, ServiceCall call)
    {
        if (arg == "me")
            return call.Session.CharacterID;

        if (int.TryParse (arg, out int charID))
            return charID;

        // try name lookup
        List <int> matches = CharacterDB.FindCharacters (arg);

        if (matches.Count == 0)
            throw new SlashError ($"Character not found: {arg}");
        if (matches.Count > 1)
            throw new SlashError ("Multiple characters match, please be more specific");

        return matches [0];
    }

    /// <summary>
    /// Moves a character to a station, updating DB and performing session change.
    /// Extracted from MoveCmd for reuse by /tr and /moveme.
    /// </summary>
    private void DoMoveToStation (int targetCharacterID, Session targetSession, Station target)
    {
        // Store location change in DB
        this.CharacterDB.UpdateStationAndLocation (
            targetCharacterID,
            target.ID,
            target.SolarSystemID,
            target.ConstellationID,
            target.RegionID
        );

        // Move active ship to new hangar
        int? shipID = targetSession.ShipID;

        if (shipID.HasValue && shipID.Value > 0)
        {
            var ship = this.Items.GetItem <ItemEntity> (shipID.Value);

            if (ship != null)
            {
                ship.LocationID = target.ID;
                ship.Flag       = Flags.Hangar;
                ship.Persist ();
            }
        }

        // CREATE A NEW SESSION CLONE
        Session newSession = Session.FromPyDictionary (targetSession);

        // Update the clone's state — NOT the live session
        newSession.StationID       = target.ID;
        newSession.LocationID      = target.ID;
        newSession.SolarSystemID2  = target.SolarSystemID;
        newSession.ConstellationID = target.ConstellationID;
        newSession.RegionID        = target.RegionID;

        // Perform REAL session change (this triggers correct client notifications)
        this.SessionManager.PerformSessionUpdate (
            "charid",
            targetCharacterID,
            newSession
        );

        Log.Information ("Slash: moved character {CharacterID} to station {StationID}", targetCharacterID, target.ID);
    }

    // =====================================================================
    //  EXISTING COMMANDS
    // =====================================================================

    private void GiveIskCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 3)
            throw new SlashError ("giveisk takes two arguments");

        string targetCharacter = argv [1];

        if (double.TryParse (argv [2], out double iskQuantity) == false)
            throw new SlashError ("giveisk second argument must be the ISK quantity to give");

        int targetCharacterID = 0;
        int originCharacterID = call.Session.CharacterID;

        if (targetCharacter == "me")
        {
            targetCharacterID = originCharacterID;
        }
        else
        {
            List <int> matches = CharacterDB.FindCharacters (targetCharacter);

            if (matches.Count > 1)
                throw new SlashError ("There's more than one character that matches the search criteria, please narrow it down");

            targetCharacterID = matches [0];
        }

        using IWallet wallet = this.Wallets.AcquireWallet (targetCharacterID, WalletKeys.MAIN);

        {
            if (iskQuantity < 0)
            {
                wallet.EnsureEnoughBalance (iskQuantity);
                wallet.CreateJournalRecord (MarketReference.GMCashTransfer, this.Items.OwnerSCC.ID, null, -iskQuantity);
            }
            else
            {
                wallet.CreateJournalRecord (MarketReference.GMCashTransfer, this.Items.OwnerSCC.ID, targetCharacterID, null, iskQuantity);
            }
        }
    }

    private void MoveCmd (string [] argv, ServiceCall call)
    {
        int targetCharacterID = call.Session.CharacterID;
        int stationID;

        // Parse args: /move <stationID> or /move <charID> <stationID>
        if (argv.Length == 2)
        {
            int.TryParse (argv [1], out stationID);
        }
        else if (argv.Length == 3)
        {
            int.TryParse (argv [1], out targetCharacterID);
            int.TryParse (argv [2], out stationID);
        }
        else
            throw new SlashError ("Usage: /move <stationID> or /move <characterID> <stationID>");

        // Find target session (must be online)
        Session targetSession =
            (targetCharacterID == call.Session.CharacterID)
                ? call.Session
                : this.SessionManager.FindSession ("charid", targetCharacterID).FirstOrDefault ();

        if (targetSession == null)
            throw new SlashError ("Target character is not online.");

        targetSession.EnsureCharacterIsInStation ();

        // Lookup station
        Station target = this.Items.GetStaticStation (stationID);

        DoMoveToStation (targetCharacterID, targetSession, target);
    }


    private void CreateCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 2)
            throw new SlashError ("create takes at least one argument");

        int typeID   = int.Parse (argv [1]);
        int quantity = 1;

        if (argv.Length > 2)
            quantity = int.Parse (argv [2]);

        call.Session.EnsureCharacterIsInStation ();

        // ensure the typeID exists
        if (this.Types.ContainsKey (typeID) == false)
            throw new SlashError ("The specified typeID doesn't exist");

        // create a new item with the correct locationID
        DogmaItems.CreateItem <ItemEntity> (Types [typeID], call.Session.CharacterID, call.Session.StationID, Flags.Hangar, quantity);
    }

    private static int ParseIntegerThatMightBeDecimal (string value)
    {
        int index = value.IndexOf ('.');

        if (index != -1)
            value = value.Substring (0, index);

        return int.Parse (value);
    }

    private void GiveSkillCmd (string [] argv, ServiceCall call)
    {
        // TODO: NOT NODE-SAFE, MUST REIMPLEMENT TAKING THAT INTO ACCOUNT!
        if (argv.Length != 4)
            throw new SlashError ("GiveSkill must have 4 arguments");

        int characterID = call.Session.CharacterID;

        string target    = argv [1].Trim ('"', ' ');
        string skillType = argv [2];
        int    level     = ParseIntegerThatMightBeDecimal (argv [3]);

        if (target != "me" && target != characterID.ToString ())
            throw new SlashError ("giveskill only supports me for now");

        Character character = this.Items.GetItem <Character> (characterID);

        if (skillType == "all")
        {
            // player wants all the skills!
            IEnumerable <KeyValuePair <int, Type>> skillTypes =
                this.Types.Where (x => x.Value.Group.Category.ID == (int) CategoryID.Skill && x.Value.Published);

            Dictionary <int, Skill> injectedSkills = character.InjectedSkillsByTypeID;

            foreach ((int typeID, Type type) in skillTypes)
                // skill already injected, train it to the desired level
                if (injectedSkills.ContainsKey (typeID))
                {
                    Skill skill = injectedSkills [typeID];

                    skill.Level = level;
                    skill.Persist ();
                    this.DogmaNotifications.QueueMultiEvent (character.ID, new OnSkillTrained (skill));
                }
                else
                {
                    Skill skill = DogmaItems.CreateItem <Skill> (type, character, character, Flags.Skill, 1, true);
                    skill.Level = level;
                    skill.Persist ();

                    DogmaNotifications.NotifyAttributeChange (character.ID, AttributeTypes.skillLevel, skill);
                    DogmaNotifications.QueueMultiEvent (character.ID, new OnSkillInjected ());

                    // add the skill history record too
                    SkillDB.CreateSkillHistoryRecord (
                        type, character, SkillHistoryReason.GMGiveSkill, skill.GetSkillPointsForLevel (level)
                    );
                }
        }
        else
        {
            Dictionary <int, Skill> injectedSkills = character.InjectedSkillsByTypeID;

            int skillTypeID = ParseIntegerThatMightBeDecimal (skillType);

            if (injectedSkills.ContainsKey (skillTypeID))
            {
                Skill skill = injectedSkills [skillTypeID];
                skill.Level = level;
                skill.Persist ();

                this.DogmaNotifications.QueueMultiEvent (character.ID, new OnSkillStartTraining (skill));
                this.DogmaNotifications.NotifyAttributeChange (character.ID, new [] {AttributeTypes.skillPoints, AttributeTypes.skillLevel}, skill);
                this.DogmaNotifications.QueueMultiEvent (character.ID, new OnSkillTrained (skill));
            }
            else
            {
                Skill skill = DogmaItems.CreateItem <Skill> (Types [skillTypeID], character, character, Flags.Skill, 1, true);
                skill.Level = level;
                skill.Persist ();

                DogmaNotifications.NotifyAttributeChange (character.ID, AttributeTypes.skillLevel, skill);
                DogmaNotifications.QueueMultiEvent (character.ID, new OnSkillInjected ());

                // add the skill history record too
                SkillDB.CreateSkillHistoryRecord (
                    Types [skillTypeID], character, SkillHistoryReason.GMGiveSkill, skill.GetSkillPointsForLevel (level)
                );

                this.DogmaNotifications.QueueMultiEvent (character.ID, new OnSkillInjected ());
            }
        }
    }

    // =====================================================================
    //  NEW COMMANDS
    // =====================================================================

    /// <summary>
    /// /heal target amount
    /// amount=0: destroy item
    /// amount>0: heal (restore HP)
    /// amount&lt;0: damage (reduce HP)
    /// </summary>
    private void HealCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 3)
            throw new SlashError ("Usage: /heal <target|itemID> <amount>");

        int targetID = ResolveItemTarget (argv [1], call);

        if (double.TryParse (argv [2], out double amount) == false)
            throw new SlashError ("heal: amount must be a number");

        if (amount == 0)
        {
            // Destroy the item
            if (this.Items.TryGetItem (targetID, out ItemEntity item) == false)
                throw new SlashError ($"Item {targetID} not found");

            // Unregister from destiny if in space
            int? solarSystemID = call.Session.SolarSystemID;

            if (solarSystemID != null && this.SolarSystemDestinyMgr.TryGet (solarSystemID.Value, out DestinyManager dm))
                dm.UnregisterEntity (targetID);

            DogmaItems.DestroyItem (item);
            Log.Information ("Slash /heal: destroyed item {ItemID}", targetID);
        }
        else
        {
            // Heal or damage - modify shield/armor/hull
            if (this.Items.TryGetItem (targetID, out ItemEntity item) == false)
                throw new SlashError ($"Item {targetID} not found");

            if (amount > 0)
            {
                // Heal: restore shield charge, remove armor damage
                if (item.Attributes.AttributeExists (AttributeTypes.shieldCharge))
                {
                    double maxShield = item.Attributes [AttributeTypes.shieldCapacity];
                    double current   = item.Attributes [AttributeTypes.shieldCharge];
                    item.Attributes [AttributeTypes.shieldCharge] = new Attribute (AttributeTypes.shieldCharge, Math.Min (current + amount, maxShield));
                }

                if (item.Attributes.AttributeExists (AttributeTypes.armorDamage))
                {
                    double current = item.Attributes [AttributeTypes.armorDamage];
                    item.Attributes [AttributeTypes.armorDamage] = new Attribute (AttributeTypes.armorDamage, Math.Max (current - amount, 0));
                }

                if (item.Attributes.AttributeExists (AttributeTypes.damage))
                {
                    double current = item.Attributes [AttributeTypes.damage];
                    item.Attributes [AttributeTypes.damage] = new Attribute (AttributeTypes.damage, Math.Max (current - amount, 0));
                }

                Log.Information ("Slash /heal: healed item {ItemID} by {Amount}", targetID, amount);
            }
            else
            {
                // Damage: reduce shield charge, increase armor damage
                double dmgAmount = -amount;

                if (item.Attributes.AttributeExists (AttributeTypes.shieldCharge))
                {
                    double current = item.Attributes [AttributeTypes.shieldCharge];
                    item.Attributes [AttributeTypes.shieldCharge] = new Attribute (AttributeTypes.shieldCharge, Math.Max (current - dmgAmount, 0));
                }

                if (item.Attributes.AttributeExists (AttributeTypes.armorDamage))
                {
                    double current = item.Attributes [AttributeTypes.armorDamage];
                    double maxArmor = item.Attributes [AttributeTypes.armorHP];
                    item.Attributes [AttributeTypes.armorDamage] = new Attribute (AttributeTypes.armorDamage, Math.Min (current + dmgAmount, maxArmor));
                }

                if (item.Attributes.AttributeExists (AttributeTypes.damage))
                {
                    double current = item.Attributes [AttributeTypes.damage];
                    double maxHP   = item.Attributes [AttributeTypes.hp];
                    item.Attributes [AttributeTypes.damage] = new Attribute (AttributeTypes.damage, Math.Min (current + dmgAmount, maxHP));
                }

                Log.Information ("Slash /heal: damaged item {ItemID} by {Amount}", targetID, dmgAmount);
            }

            item.Persist ();
        }
    }

    /// <summary>
    /// /unload target typeID|all - Unload modules from a ship to cargo
    /// </summary>
    private void UnloadCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 3)
            throw new SlashError ("Usage: /unload <target|me> <moduleTypeID|all>");

        int shipID = ResolveItemTarget (argv [1], call);
        string moduleArg = argv [2];

        Ship ship = this.Items.LoadItem <Ship> (shipID);

        if (ship == null)
            throw new SlashError ($"Ship {shipID} not found or is not a ship");

        int unloaded = 0;

        foreach (var kvp in ship.Items)
        {
            ItemEntity module = kvp.Value;

            if (!module.IsInModuleSlot () && !module.IsInRigSlot ())
                continue;

            if (moduleArg != "all")
            {
                if (int.TryParse (moduleArg, out int filterTypeID) == false)
                    throw new SlashError ("moduleTypeID must be a number or 'all'");

                if (module.Type.ID != filterTypeID)
                    continue;
            }

            DogmaItems.MoveItem (module, Flags.Cargo);
            unloaded++;
        }

        Log.Information ("Slash /unload: unloaded {Count} modules from ship {ShipID}", unloaded, shipID);
    }

    /// <summary>
    /// /spawn typeID [rest] - Spawn an entity in space at the caller's position
    /// </summary>
    private void SpawnCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 2)
            throw new SlashError ("Usage: /spawn <typeID>");

        int typeID = int.Parse (argv [1]);

        if (this.Types.ContainsKey (typeID) == false)
            throw new SlashError ("The specified typeID doesn't exist");

        int solarSystemID = call.Session.EnsureCharacterIsInSpace ();
        int shipID        = call.Session.ShipID ?? 0;

        // Get the caller's position
        double x = 0, y = 0, z = 0;

        if (shipID != 0 && this.SolarSystemDestinyMgr.TryGet (solarSystemID, out DestinyManager dm) &&
            dm.TryGetEntity (shipID, out BubbleEntity shipEnt))
        {
            x = shipEnt.Position.X + 5000;
            y = shipEnt.Position.Y;
            z = shipEnt.Position.Z;
        }

        Type type = Types [typeID];

        // Create the item in the solar system
        ItemEntity newItem = DogmaItems.CreateItem <ItemEntity> (
            type, call.Session.CharacterID, solarSystemID, Flags.None, 1, true
        );

        // Set position
        newItem.X = x;
        newItem.Y = y;
        newItem.Z = z;
        newItem.Persist ();

        // Register in DestinyManager
        if (this.SolarSystemDestinyMgr.TryGet (solarSystemID, out DestinyManager destinyMgr))
        {
            bool isShipCategory = type.Group.Category.ID == (int) CategoryID.Ship;

            var bubble = new BubbleEntity
            {
                ItemID        = newItem.ID,
                TypeID        = type.ID,
                GroupID       = type.Group.ID,
                CategoryID    = type.Group.Category.ID,
                Name          = type.Name,
                OwnerID       = call.Session.CharacterID,
                CorporationID = call.Session.CorporationID,
                AllianceID    = 0,
                CharacterID   = 0,
                Position      = new Vector3 { X = x, Y = y, Z = z },
                Velocity      = default,
                Mode          = isShipCategory ? BallMode.Stop : BallMode.Rigid,
                Flags         = isShipCategory
                    ? BallFlag.IsFree | BallFlag.IsMassive | BallFlag.IsInteractive
                    : BallFlag.IsGlobal | BallFlag.IsMassive | BallFlag.IsInteractive,
                Radius        = type.Radius,
                Mass          = 1000000.0,
                MaxVelocity   = isShipCategory ? 200.0 : 0.0,
                SpeedFraction = 0.0,
                Agility       = 1.0
            };

            destinyMgr.RegisterEntity (bubble);
        }

        Log.Information ("Slash /spawn: spawned typeID={TypeID} as itemID={ItemID} at ({X:F0},{Y:F0},{Z:F0})",
            typeID, newItem.ID, x, y, z);
    }

    /// <summary>
    /// /fit target typeID [qty] - Fit a module to a ship
    /// </summary>
    private void FitCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 3)
            throw new SlashError ("Usage: /fit <target|me> <typeID> [qty]");

        int shipID = ResolveItemTarget (argv [1], call);
        int typeID = int.Parse (argv [2]);
        int qty    = argv.Length > 3 ? int.Parse (argv [3]) : 1;

        if (this.Types.ContainsKey (typeID) == false)
            throw new SlashError ("The specified typeID doesn't exist");

        Type type       = Types [typeID];
        int  categoryID = type.Group.Category.ID;

        // Determine target flag based on category/group
        Flags targetFlag;

        if (categoryID == (int) CategoryID.Charge)
            targetFlag = Flags.Cargo;
        else if (categoryID == (int) CategoryID.Drone)
            targetFlag = Flags.DroneBay;
        else
            targetFlag = Flags.Cargo;

        for (int i = 0; i < qty; i++)
        {
            ItemEntity module = DogmaItems.CreateItem <ItemEntity> (
                type, call.Session.CharacterID, shipID, targetFlag, 1, true
            );

            // If it's a module, try to fit it into an appropriate slot
            if (categoryID == (int) CategoryID.Module)
            {
                try
                {
                    DogmaItems.FitInto (module, shipID, FindFreeSlot (shipID, type), call.Session);
                }
                catch
                {
                    // If fitting fails, leave in cargo
                    Log.Warning ("Slash /fit: could not fit typeID={TypeID} into slot, left in cargo", typeID);
                }
            }
        }

        Log.Information ("Slash /fit: fitted {Qty}x typeID={TypeID} to ship {ShipID}", qty, typeID, shipID);
    }

    /// <summary>
    /// Find a free module slot on a ship for the given module type.
    /// </summary>
    private Flags FindFreeSlot (int shipID, Type moduleType)
    {
        Ship ship = this.Items.LoadItem <Ship> (shipID);
        int  effectCategory = GetModuleSlotCategory (moduleType);

        Flags startFlag, endFlag;

        switch (effectCategory)
        {
            case 1: // hi slot
                startFlag = Flags.HiSlot0;
                endFlag   = Flags.HiSlot7;
                break;
            case 2: // med slot
                startFlag = Flags.MedSlot0;
                endFlag   = Flags.MedSlot7;
                break;
            case 3: // lo slot
                startFlag = Flags.LoSlot0;
                endFlag   = Flags.LoSlot7;
                break;
            default:
                return Flags.Cargo;
        }

        var usedFlags = new HashSet <Flags> ();

        foreach (var kvp in ship.Items)
        {
            if (kvp.Value.IsInModuleSlot ())
                usedFlags.Add (kvp.Value.Flag);
        }

        for (Flags f = startFlag; f <= endFlag; f++)
        {
            if (!usedFlags.Contains (f))
                return f;
        }

        return Flags.Cargo;
    }

    /// <summary>
    /// Determine module slot category: 1=hi, 2=med, 3=lo.
    /// Uses effect list if available, otherwise defaults based on group.
    /// </summary>
    private static int GetModuleSlotCategory (Type moduleType)
    {
        // Default heuristic: weapons/launchers are high slot, shield/propulsion are med, armor/engineering are low
        // In a full implementation this would check the item's effects for hiPower/medPower/loPower
        int groupID = moduleType.Group.ID;

        // Common high slot groups: energy weapons, hybrid weapons, projectile weapons, missile launchers
        if (groupID >= 53 && groupID <= 56) return 1;  // weapon groups
        if (groupID == 72 || groupID == 73) return 1;   // missile launchers
        if (groupID == 507 || groupID == 508) return 1;  // turrets

        // Common med slot groups: shield, propulsion, ECM, sensor
        if (groupID == 38 || groupID == 39 || groupID == 40) return 2;  // shield related
        if (groupID == 46) return 2;  // propulsion

        // Default to low slot
        return 3;
    }

    /// <summary>
    /// /online target - Online all fitted modules on a ship
    /// </summary>
    private void OnlineCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 2)
            throw new SlashError ("Usage: /online <target|me>");

        int shipID = ResolveItemTarget (argv [1], call);
        Ship ship  = this.Items.LoadItem <Ship> (shipID);

        if (ship == null)
            throw new SlashError ($"Ship {shipID} not found");

        int onlined = 0;

        foreach (var kvp in ship.Items)
        {
            ItemEntity module = kvp.Value;

            if (!module.IsInModuleSlot ())
                continue;

            try
            {
                if (module.Attributes.AttributeExists (AttributeTypes.isOnline) &&
                    module.Attributes [AttributeTypes.isOnline] == 0)
                {
                    module.Attributes [AttributeTypes.isOnline] = new Attribute (AttributeTypes.isOnline, 1);
                    module.Persist ();
                    onlined++;
                }
            }
            catch
            {
                // skip modules that fail to online
            }
        }

        Log.Information ("Slash /online: onlined {Count} modules on ship {ShipID}", onlined, shipID);
    }

    /// <summary>
    /// /tr target destination [offset=x,y,z]
    /// Teleport to a station (reuses DoMoveToStation) or to another character's position.
    /// </summary>
    private void TrCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 3)
            throw new SlashError ("Usage: /tr <target|me> <stationID|charID>");

        string targetArg = argv [1];
        string destArg   = argv [2];

        int targetCharacterID = ResolveCharacterTarget (targetArg, call);

        Session targetSession =
            (targetCharacterID == call.Session.CharacterID)
                ? call.Session
                : this.SessionManager.FindSession ("charid", targetCharacterID).FirstOrDefault ();

        if (targetSession == null)
            throw new SlashError ("Target character is not online.");

        if (int.TryParse (destArg, out int destID) == false)
            throw new SlashError ("Destination must be a numeric ID (stationID or characterID)");

        // Try as station first
        try
        {
            Station station = this.Items.GetStaticStation (destID);

            // It's a valid station - move there
            DoMoveToStation (targetCharacterID, targetSession, station);
            return;
        }
        catch
        {
            // Not a station, try as character
        }

        // Try as another character - teleport to their position in space
        Session destSession = this.SessionManager.FindSession ("charid", destID).FirstOrDefault ();

        if (destSession != null)
        {
            int? destSystemID = destSession.SolarSystemID;

            if (destSystemID == null)
                throw new SlashError ("Destination character is not in space");

            int? destShipID = destSession.ShipID;

            if (destShipID != null && destShipID.Value != 0 &&
                this.SolarSystemDestinyMgr.TryGet (destSystemID.Value, out DestinyManager dm) &&
                dm.TryGetEntity (destShipID.Value, out BubbleEntity destEnt))
            {
                // Teleport target's ship to destination character's position
                int? myShipID   = targetSession.ShipID;
                int? mySystemID = targetSession.SolarSystemID;

                if (myShipID == null || myShipID.Value == 0)
                    throw new SlashError ("You don't have an active ship");

                if (mySystemID != null && this.SolarSystemDestinyMgr.TryGet (mySystemID.Value, out DestinyManager srcDm))
                    srcDm.UnregisterEntity (myShipID.Value);

                // Update ship position
                if (this.Items.TryGetItem (myShipID.Value, out ItemEntity shipEntity))
                {
                    shipEntity.X = destEnt.Position.X + 5000;
                    shipEntity.Y = destEnt.Position.Y;
                    shipEntity.Z = destEnt.Position.Z;
                    shipEntity.Persist ();
                }

                // Session change if different system
                if (mySystemID != destSystemID)
                {
                    Session newSession = Session.FromPyDictionary (targetSession);
                    newSession.SolarSystemID2 = destSystemID.Value;
                    newSession.LocationID     = destSystemID.Value;

                    this.SessionManager.PerformSessionUpdate ("charid", targetCharacterID, newSession);
                }

                Log.Information ("Slash /tr: teleported {CharID} to character {DestCharID}", targetCharacterID, destID);
                return;
            }
        }

        throw new SlashError ($"Could not resolve destination {destID} as station or online character");
    }

    /// <summary>
    /// /removeskill target typeID|all - Remove skills from a character
    /// </summary>
    private void RemoveSkillCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 3)
            throw new SlashError ("Usage: /removeskill <target|me> <typeID|all>");

        int    characterID = ResolveCharacterTarget (argv [1], call);
        string skillArg    = argv [2];

        Character character = this.Items.GetItem <Character> (characterID);
        Dictionary <int, Skill> injectedSkills = character.InjectedSkillsByTypeID;

        if (skillArg == "all")
        {
            foreach (var kvp in injectedSkills)
            {
                DogmaItems.DestroyItem (kvp.Value);
            }

            DogmaNotifications.QueueMultiEvent (character.ID, new OnSkillInjected ());
            Log.Information ("Slash /removeskill: removed all skills from character {CharID}", characterID);
        }
        else
        {
            int skillTypeID = ParseIntegerThatMightBeDecimal (skillArg);

            if (injectedSkills.TryGetValue (skillTypeID, out Skill skill) == false)
                throw new SlashError ($"Character does not have skill typeID {skillTypeID}");

            DogmaItems.DestroyItem (skill);
            DogmaNotifications.QueueMultiEvent (character.ID, new OnSkillInjected ());
            Log.Information ("Slash /removeskill: removed skill typeID={TypeID} from character {CharID}", skillTypeID, characterID);
        }
    }

    /// <summary>
    /// /spawnn qty deviation typeID - Spawn multiple entities scattered within a radius
    /// </summary>
    private void SpawnNCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 4)
            throw new SlashError ("Usage: /spawnn <qty> <deviation> <typeID>");

        int    qty       = int.Parse (argv [1]);
        double deviation = double.Parse (argv [2]);
        int    typeID    = int.Parse (argv [3]);

        if (qty <= 0 || qty > 100)
            throw new SlashError ("qty must be between 1 and 100");

        if (this.Types.ContainsKey (typeID) == false)
            throw new SlashError ("The specified typeID doesn't exist");

        int solarSystemID = call.Session.EnsureCharacterIsInSpace ();
        int shipID        = call.Session.ShipID ?? 0;

        double baseX = 0, baseY = 0, baseZ = 0;

        if (shipID != 0 && this.SolarSystemDestinyMgr.TryGet (solarSystemID, out DestinyManager dm) &&
            dm.TryGetEntity (shipID, out BubbleEntity shipEnt))
        {
            baseX = shipEnt.Position.X;
            baseY = shipEnt.Position.Y;
            baseZ = shipEnt.Position.Z;
        }

        Type type   = Types [typeID];
        var  random = new Random ();

        for (int i = 0; i < qty; i++)
        {
            double ox = (random.NextDouble () * 2 - 1) * deviation;
            double oy = (random.NextDouble () * 2 - 1) * deviation;
            double oz = (random.NextDouble () * 2 - 1) * deviation;
            double x  = baseX + ox;
            double y  = baseY + oy;
            double z  = baseZ + oz;

            ItemEntity newItem = DogmaItems.CreateItem <ItemEntity> (
                type, call.Session.CharacterID, solarSystemID, Flags.None, 1, true
            );

            newItem.X = x;
            newItem.Y = y;
            newItem.Z = z;
            newItem.Persist ();

            if (this.SolarSystemDestinyMgr.TryGet (solarSystemID, out DestinyManager destinyMgr))
            {
                var bubble = new BubbleEntity
                {
                    ItemID        = newItem.ID,
                    TypeID        = type.ID,
                    GroupID       = type.Group.ID,
                    CategoryID    = type.Group.Category.ID,
                    Name          = type.Name,
                    OwnerID       = call.Session.CharacterID,
                    CorporationID = call.Session.CorporationID,
                    AllianceID    = 0,
                    CharacterID   = 0,
                    Position      = new Vector3 { X = x, Y = y, Z = z },
                    Velocity      = default,
                    Mode          = BallMode.Rigid,
                    Flags         = BallFlag.IsGlobal | BallFlag.IsMassive | BallFlag.IsInteractive,
                    Radius        = type.Radius,
                    Mass          = 1000000.0,
                    MaxVelocity   = 0.0,
                    SpeedFraction = 0.0,
                    Agility       = 1.0
                };

                destinyMgr.RegisterEntity (bubble);
            }
        }

        Log.Information ("Slash /spawnn: spawned {Qty}x typeID={TypeID} with deviation={Deviation}", qty, typeID, deviation);
    }

    /// <summary>
    /// /entity deploy qty typeID - Spawn NPC entities
    /// </summary>
    private void EntityCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 4 || argv [1] != "deploy")
            throw new SlashError ("Usage: /entity deploy <qty> <typeID>");

        int qty    = int.Parse (argv [2]);
        int typeID = int.Parse (argv [3]);

        if (qty <= 0 || qty > 100)
            throw new SlashError ("qty must be between 1 and 100");

        if (this.Types.ContainsKey (typeID) == false)
            throw new SlashError ("The specified typeID doesn't exist");

        Type type = Types [typeID];

        if (type.Group.Category.ID != (int) CategoryID.Entity)
            throw new SlashError ($"typeID {typeID} is not an Entity (category {type.Group.Category.ID})");

        int solarSystemID = call.Session.EnsureCharacterIsInSpace ();
        int shipID        = call.Session.ShipID ?? 0;

        double baseX = 0, baseY = 0, baseZ = 0;

        if (shipID != 0 && this.SolarSystemDestinyMgr.TryGet (solarSystemID, out DestinyManager dm) &&
            dm.TryGetEntity (shipID, out BubbleEntity shipEnt))
        {
            baseX = shipEnt.Position.X;
            baseY = shipEnt.Position.Y;
            baseZ = shipEnt.Position.Z;
        }

        var random = new Random ();

        for (int i = 0; i < qty; i++)
        {
            double x = baseX + (random.NextDouble () * 2 - 1) * 10000;
            double y = baseY + (random.NextDouble () * 2 - 1) * 10000;
            double z = baseZ + (random.NextDouble () * 2 - 1) * 10000;

            ItemEntity newItem = DogmaItems.CreateItem <ItemEntity> (
                type, 1, solarSystemID, Flags.None, 1, true
            );

            newItem.X = x;
            newItem.Y = y;
            newItem.Z = z;
            newItem.Persist ();

            if (this.SolarSystemDestinyMgr.TryGet (solarSystemID, out DestinyManager destinyMgr))
            {
                var bubble = new BubbleEntity
                {
                    ItemID        = newItem.ID,
                    TypeID        = type.ID,
                    GroupID       = type.Group.ID,
                    CategoryID    = type.Group.Category.ID,
                    Name          = type.Name,
                    OwnerID       = 1,
                    CorporationID = 0,
                    AllianceID    = 0,
                    CharacterID   = 0,
                    Position      = new Vector3 { X = x, Y = y, Z = z },
                    Velocity      = default,
                    Mode          = BallMode.Stop,
                    Flags         = BallFlag.IsFree | BallFlag.IsMassive | BallFlag.IsInteractive,
                    Radius        = type.Radius,
                    Mass          = 1000000.0,
                    MaxVelocity   = 200.0,
                    SpeedFraction = 0.0,
                    Agility       = 1.0
                };

                destinyMgr.RegisterEntity (bubble);
            }
        }

        Log.Information ("Slash /entity: deployed {Qty}x typeID={TypeID}", qty, typeID);
    }

    /// <summary>
    /// /bp typeID [runs] [me] [pe] - Create a blueprint
    /// </summary>
    private void BpCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 2)
            throw new SlashError ("Usage: /bp <typeID> [runs] [me] [pe]");

        int typeID = int.Parse (argv [1]);

        if (this.Types.ContainsKey (typeID) == false)
            throw new SlashError ("The specified typeID doesn't exist");

        call.Session.EnsureCharacterIsInStation ();

        ItemEntity bp = DogmaItems.CreateItem <ItemEntity> (
            Types [typeID], call.Session.CharacterID, call.Session.StationID, Flags.Hangar, 1, true
        );

        Log.Information ("Slash /bp: created blueprint typeID={TypeID} as itemID={ItemID}", typeID, bp.ID);
    }

    /// <summary>
    /// /load target typeID qty - Create items directly into a container's cargo
    /// </summary>
    private void LoadCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 4)
            throw new SlashError ("Usage: /load <target|me> <typeID> <qty>");

        int containerID = ResolveItemTarget (argv [1], call);
        int typeID      = int.Parse (argv [2]);
        int qty         = int.Parse (argv [3]);

        if (this.Types.ContainsKey (typeID) == false)
            throw new SlashError ("The specified typeID doesn't exist");

        DogmaItems.CreateItem <ItemEntity> (
            Types [typeID], call.Session.CharacterID, containerID, Flags.Cargo, qty
        );

        Log.Information ("Slash /load: loaded {Qty}x typeID={TypeID} into container {ContainerID}", qty, typeID, containerID);
    }

    /// <summary>
    /// /repairmodules - Repair all modules on the caller's ship
    /// </summary>
    private void RepairModulesCmd (string [] argv, ServiceCall call)
    {
        int? shipID = call.Session.ShipID;

        if (shipID == null || shipID.Value == 0)
            throw new SlashError ("You don't have an active ship");

        Ship ship = this.Items.LoadItem <Ship> (shipID.Value);

        if (ship == null)
            throw new SlashError ("Could not load ship");

        int repaired = 0;

        foreach (var kvp in ship.Items)
        {
            ItemEntity module = kvp.Value;

            if (!module.IsInModuleSlot () && !module.IsInRigSlot ())
                continue;

            if (module.Attributes.AttributeExists (AttributeTypes.damage))
            {
                module.Attributes [AttributeTypes.damage] = new Attribute (AttributeTypes.damage, 0);
                module.Persist ();
                repaired++;
            }
        }

        Log.Information ("Slash /repairmodules: repaired {Count} modules on ship {ShipID}", repaired, shipID.Value);
    }

    /// <summary>
    /// /setstanding fromID toID value reason - Set NPC standings
    /// </summary>
    private void SetStandingCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 5)
            throw new SlashError ("Usage: /setstanding <fromID> <toID> <value> <reason>");

        int    fromID = int.Parse (argv [1]);
        int    toID   = int.Parse (argv [2]);
        double value  = double.Parse (argv [3]);
        string reason = string.Join (" ", argv.Skip (4));

        this.Standings.SetStanding (EventType.StandingSlashSet, fromID, toID, value, reason);
        Log.Information ("Slash /setstanding: {FromID} -> {ToID} = {Value} ({Reason})", fromID, toID, value, reason);
    }

    /// <summary>
    /// /unspawn [range=N] - Destroy spawned entities within range
    /// Supports: /unspawn, /unspawn range=50000
    /// </summary>
    private void UnspawnCmd (string [] argv, ServiceCall call)
    {
        int solarSystemID = call.Session.EnsureCharacterIsInSpace ();
        int shipID        = call.Session.ShipID ?? 0;

        double range = 50000; // default 50km

        // Parse optional arguments
        for (int i = 1; i < argv.Length; i++)
        {
            if (argv [i].StartsWith ("range="))
            {
                double.TryParse (argv [i].Substring (6), out range);
            }
        }

        if (!this.SolarSystemDestinyMgr.TryGet (solarSystemID, out DestinyManager dm))
            throw new SlashError ("No destiny manager for this solar system");

        Vector3 myPos = default;

        if (shipID != 0 && dm.TryGetEntity (shipID, out BubbleEntity myShip))
            myPos = myShip.Position;

        int destroyed = 0;
        var toRemove  = new List <int> ();

        foreach (BubbleEntity entity in dm.GetEntities ())
        {
            // Skip player ships and celestials (stations, planets, etc.)
            if (entity.IsPlayer)
                continue;
            if (entity.ItemID == shipID)
                continue;

            double dist = myPos.Distance (entity.Position);

            if (dist > range)
                continue;

            toRemove.Add (entity.ItemID);
        }

        foreach (int itemID in toRemove)
        {
            dm.UnregisterEntity (itemID);

            if (this.Items.TryGetItem (itemID, out ItemEntity item))
                DogmaItems.DestroyItem (item);

            destroyed++;
        }

        Log.Information ("Slash /unspawn: destroyed {Count} entities within {Range}m in system {SystemID}", destroyed, range, solarSystemID);
    }

    /// <summary>
    /// /moveme stationID - Simple alias for /move, moves the caller to a station
    /// </summary>
    private void MoveMeCmd (string [] argv, ServiceCall call)
    {
        if (argv.Length < 2)
            throw new SlashError ("Usage: /moveme <stationID>");

        if (int.TryParse (argv [1], out int stationID) == false)
            throw new SlashError ("stationID must be a number");

        Station target = this.Items.GetStaticStation (stationID);
        DoMoveToStation (call.Session.CharacterID, call.Session, target);
    }
}
