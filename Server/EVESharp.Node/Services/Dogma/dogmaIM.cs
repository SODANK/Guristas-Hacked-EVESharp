using System;
using EVESharp.Database;
using EVESharp.Database.Account;
using EVESharp.Database.Extensions;
using EVESharp.Database.Inventory;
using EVESharp.Database.Inventory.Attributes;
using EVESharp.Database.Inventory.Groups;
using EVESharp.Database.Types;
using EVESharp.EVE.Data.Inventory;
using EVESharp.EVE.Data.Inventory.Items;
using EVESharp.EVE.Data.Inventory.Items.Types;
using EVESharp.EVE.Exceptions;
using EVESharp.EVE.Exceptions.inventory;
using EVESharp.EVE.Network.Services;
using EVESharp.EVE.Network.Services.Validators;
using EVESharp.EVE.Notifications;
using EVESharp.EVE.Notifications.Station;
using EVESharp.EVE.Packets.Complex;
using EVESharp.EVE.Sessions;
using EVESharp.EVE.Types;
using EVESharp.Node.Dogma;
using EVESharp.Types;
using EVESharp.Types.Collections;

namespace EVESharp.Node.Services.Dogma;

[MustBeCharacter]
public class dogmaIM : ClientBoundService
{
    public override AccessLevel AccessLevel => AccessLevel.None;

    private IItems              Items             { get; }
    private IAttributes         Attributes        => this.Items.Attributes;
    private IDefaultAttributes  DefaultAttributes => this.Items.DefaultAttributes;
    private ISolarSystems       SolarSystems      { get; }
    private INotificationSender Notifications     { get; }
    private EffectsManager      EffectsManager    { get; }
    private IDatabase Database          { get; }

    public dogmaIM
    (
        EffectsManager effectsManager, IItems items, INotificationSender notificationSender, IBoundServiceManager manager, IDatabase database,
        ISolarSystems  solarSystems
    ) : base (manager)
    {
        EffectsManager = effectsManager;
        Items          = items;
        Notifications  = notificationSender;
        Database       = database;
        SolarSystems   = solarSystems;
    }

    protected dogmaIM
    (
        int     locationID, EffectsManager effectsManager, IItems items, INotificationSender notificationSender, IBoundServiceManager manager,
        Session session,    ISolarSystems  solarSystems
    ) : base (manager, session, locationID)
    {
        EffectsManager = effectsManager;
        Items          = items;
        Notifications  = notificationSender;
        SolarSystems   = solarSystems;
    }

    public PyDataType ShipGetInfo (ServiceCall call)
    {
        int  callerCharacterID = call.Session.CharacterID;
        int? shipID            = call.Session.ShipID;

        if (shipID is null)
            throw new CustomError ("The character is not aboard any ship");

        Ship ship = this.Items.LoadItem <Ship> ((int) shipID);

        if (ship is null)
            throw new CustomError ($"Cannot get information for ship {call.Session.ShipID}");

        ship.EnsureOwnership (callerCharacterID, call.Session.CorporationID, call.Session.CorporationRole, true);

        // Ensure shieldCharge is set to shieldCapacity if not already present.
        // Unlike armor (armorDamage defaults to 0 = full) and structure (damage defaults to 0 = full),
        // shields use shieldCharge which defaults to 0 = EMPTY. We must initialize it.
        if (!ship.Attributes.TryGetAttribute (AttributeTypes.shieldCharge, out _))
        {
            double shieldCap = ship.Attributes [AttributeTypes.shieldCapacity];
            ship.Attributes [AttributeTypes.shieldCharge] = new EVESharp.Database.Inventory.Attributes.Attribute (AttributeTypes.shieldCharge, shieldCap);
        }

        // Ensure warp attributes are set. Without baseWarpSpeed the client's godma proxy
        // calculates warp range as 0, causing "Out of warp range" for all targets.
        if (!ship.Attributes.TryGetAttribute (AttributeTypes.baseWarpSpeed, out _))
        {
            ship.Attributes [AttributeTypes.baseWarpSpeed] = new EVESharp.Database.Inventory.Attributes.Attribute (AttributeTypes.baseWarpSpeed, 1.0);
        }
        if (!ship.Attributes.TryGetAttribute (AttributeTypes.warpSpeedMultiplier, out _))
        {
            ship.Attributes [AttributeTypes.warpSpeedMultiplier] = new EVESharp.Database.Inventory.Attributes.Attribute (AttributeTypes.warpSpeedMultiplier, 1.0);
        }

        ItemInfo itemInfo = new ItemInfo ();
        itemInfo.AddRow (ship.ID, ship.GetEntityRow (), ship.GetEffects (), ship.Attributes, DateTime.UtcNow.ToFileTime ());

        foreach ((int _, ItemEntity item) in ship.Items)
        {
            if (item.IsInModuleSlot () == false && item.IsInRigSlot () == false)
                continue;

            itemInfo.AddRow (
                item.ID,
                item.GetEntityRow (),
                item.GetEffects (),
                item.Attributes,
                DateTime.UtcNow.ToFileTime ()
            );
        }

        return itemInfo;
    }

    public PyDataType CharGetInfo (ServiceCall call)
    {
        int callerCharacterID = call.Session.CharacterID;
        Character character = this.Items.GetItem <Character> (callerCharacterID);

        if (character is null)
            throw new CustomError ($"Cannot get information for character {callerCharacterID}");

        ItemInfo itemInfo = new ItemInfo ();
        itemInfo.AddRow (character.ID, character.GetEntityRow (), character.GetEffects (), character.Attributes, DateTime.UtcNow.ToFileTime ());

        foreach ((int _, ItemEntity item) in character.Items)
            switch (item.Flag)
            {
                case Flags.Booster:
                case Flags.Implant:
                case Flags.Skill:
                case Flags.SkillInTraining:
                    itemInfo.AddRow (
                        item.ID,
                        item.GetEntityRow (),
                        item.GetEffects (),
                        item.Attributes,
                        DateTime.UtcNow.ToFileTime ()
                    );
                    break;
            }

        return itemInfo;
    }

    public PyDataType ItemGetInfo (ServiceCall call, PyInteger itemID)
    {
        int callerCharacterID = call.Session.CharacterID;
        ItemEntity item = this.Items.LoadItem (itemID);

        if (item.ID != callerCharacterID && item.OwnerID != callerCharacterID && item.OwnerID != call.Session.CorporationID)
            throw new TheItemIsNotYoursToTake (itemID);

        return new Row (
            new PyList <PyString> (5)
            {
                [0] = "itemID",
                [1] = "invItem",
                [2] = "activeEffects",
                [3] = "attributes",
                [4] = "time"
            },
            new PyList (5)
            {
                [0] = item.ID,
                [1] = item.GetEntityRow (),
                [2] = item.GetEffects (),
                [3] = item.Attributes,
                [4] = DateTime.UtcNow.ToFileTimeUtc ()
            }
        );
    }

    public PyDataType GetWeaponBankInfoForShip (ServiceCall call)
    {
        return new PyDictionary ();
    }

    public PyDataType GetCharacterBaseAttributes (ServiceCall call)
    {
        int callerCharacterID = call.Session.CharacterID;
        Character character = this.Items.GetItem <Character> (callerCharacterID);

        if (character is null)
            throw new CustomError ($"Cannot get information for character {callerCharacterID}");

        return new PyDictionary
        {
            [(int) AttributeTypes.willpower]    = character.Willpower,
            [(int) AttributeTypes.charisma]     = character.Charisma,
            [(int) AttributeTypes.intelligence] = character.Intelligence,
            [(int) AttributeTypes.perception]   = character.Perception,
            [(int) AttributeTypes.memory]       = character.Memory
        };
    }

    public PyDataType LogAttribute (ServiceCall call, PyInteger itemID, PyInteger attributeID)
    {
        return this.LogAttribute (call, itemID, attributeID, "");
    }

    public PyList <PyString> LogAttribute (ServiceCall call, PyInteger itemID, PyInteger attributeID, PyString reason)
    {
        ulong role     = call.Session.Role;
        ulong roleMask = (ulong) (Roles.ROLE_GDH | Roles.ROLE_QA | Roles.ROLE_PROGRAMMER | Roles.ROLE_GMH);

        if ((role & roleMask) == 0)
            throw new CustomError ("Not allowed!");

        ItemEntity item = this.Items.GetItem (itemID);

        if (item.Attributes.AttributeExists (attributeID) == false)
            throw new CustomError ("The given attribute doesn't exists in the item");

        return new PyList <PyString> (5)
        {
            [0] = null,
            [1] = null,
            [2] = $"Server value: {item.Attributes [attributeID]}",
            [3] = $"Base value: {this.DefaultAttributes [item.Type.ID] [attributeID]}",
            [4] = $"Reason: {reason}"
        };
    }

    public PyDataType Activate (ServiceCall call, PyInteger itemID, PyString effectName, PyDataType target, PyDataType repeat)
    {
        ShipModule module = this.Items.GetItem <ShipModule> (itemID);
        EffectsManager.GetForItem (module, call.Session).ApplyEffect (effectName, call.Session);
        return null;
    }

    public PyDataType Deactivate (ServiceCall call, PyInteger itemID, PyString effectName)
    {
        ShipModule module = this.Items.GetItem <ShipModule> (itemID);
        EffectsManager.GetForItem (module, call.Session).StopApplyingEffect (effectName, call.Session);
        return null;
    }

    // === NEW STUBS ADDED BELOW ===

    public PyDataType CheckSendLocationInfo(ServiceCall call)
    {
        Console.WriteLine("[dogmaIM] CheckSendLocationInfo called");
        // Client expects a response but doesn't need data yet
        return null;
    }

    public PyDataType GetTargets(ServiceCall call)
    {
        Console.WriteLine("[dogmaIM] GetTargets called");
        // Return empty list; client interprets this as "no current targets"
        return new PyList();
    }

    // === EXISTING OVERRIDES BELOW ===

    protected override long MachoResolveObject (ServiceCall call, ServiceBindParams parameters)
    {
        return parameters.ExtraValue switch
        {
            (int) GroupID.SolarSystem => Database.CluResolveAddress ("solarsystem", parameters.ObjectID),
            (int) GroupID.Station     => Database.CluResolveAddress ("station",     parameters.ObjectID),
            _                         => throw new CustomError ("Unknown item's groupID")
        };
    }

    protected override BoundService CreateBoundInstance (ServiceCall call, ServiceBindParams bindParams)
    {
        int characterID = call.Session.CharacterID;

        if (this.MachoResolveObject (call, bindParams) != BoundServiceManager.MachoNet.NodeID)
            throw new CustomError ("Trying to bind an object that does not belong to us!");

        Character character = this.Items.LoadItem <Character> (characterID);

        if (bindParams.ExtraValue == (int) GroupID.Station && call.Session.StationID == bindParams.ObjectID)
        {
            this.Items.GetStaticStation (bindParams.ObjectID).Guests [characterID] = character;
            Notifications.NotifyStation (bindParams.ObjectID, new OnCharNowInStation (call.Session));
        }

        return new dogmaIM (bindParams.ObjectID, EffectsManager, Items, Notifications, BoundServiceManager, call.Session, SolarSystems);
    }

    protected override void OnClientDisconnected ()
    {
        int characterID = Session.CharacterID;

        if (Session.StationID == ObjectID)
        {
            this.Items.GetStaticStation (ObjectID).Guests.Remove (characterID);
            Notifications.NotifyStation (ObjectID, new OnCharNoLongerInStation (Session));
            this.Items.UnloadItem (characterID);
            if (Session.ShipID is not null)
                this.Items.UnloadItem ((int) Session.ShipID);
        }
    }

    // in EVESharp.Node.Services.Dogma.dogmaIM

public PyDataType GetTargeters(ServiceCall call)
{
    Console.WriteLine("[dogmaIM] GetTargeters called (stubbed, returning empty list)");

    // In real EVE this would return who is targeting you.
    // For now, we just hand the client an empty list so it shuts up.
    return new PyList();
}

}
