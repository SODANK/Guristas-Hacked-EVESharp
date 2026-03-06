using System;
using System.Collections.Generic;
using System.Data.Common;
using EVESharp.Database;
using EVESharp.Database.Inventory;
using EVESharp.Database.Inventory.Groups;
using EVESharp.Database.Types;
using EVESharp.Destiny;
using EVESharp.EVE.Data.Inventory;
using EVESharp.EVE.Data.Inventory.Items;
using EVESharp.EVE.Data.Inventory.Items.Types;
using EVESharp.EVE.Network.Services;
using EVESharp.EVE.Notifications;
using EVESharp.EVE.Sessions;
using EVESharp.Types;
using EVESharp.Types.Collections;
using Serilog;

namespace EVESharp.Node.Services.Space
{
    /// <summary>
    /// The 'beyonce' service handles all ballpark / Destiny state for space gameplay.
    ///
    /// CLIENT FLOW (from decompiled michelle.py / eveMoniker.py):
    ///   1. ship.Undock() sends session change (stationid=None, solarsystemid=X)
    ///      + ship.Undock() also sends DoDestinyUpdate notification (PRIMARY delivery)
    ///   2. Client gameui.OnSessionChanged → GoInflight → michelle.AddBallpark(ssid)
    ///   3. AddBallpark creates native destiny.Ballpark, then:
    ///      a. Park.__init__() calls moniker.GetBallPark(ssid) → Moniker('beyonce', ssid)
    ///      b. remoteBallpark.Bind() → MachoBindObject → SERVER creates BOUND beyonce instance
    ///         (BACKUP: bound constructor also sends DoDestinyUpdate)
    ///      c. eve.RemoteSvc('beyonce').GetFormations() → calls GLOBAL service (just returns formations)
    ///      d. __bp.LoadFormations(formations)
    ///      e. __bp.Start() → starts tick loop, processes queued DoDestinyUpdate
    ///   4. Park.SetState(bag) → reads destiny binary → creates balls → DoBallsAdded → 3D render
    ///
    /// NOTE: DoDestinyUpdate is sent from ship.Undock() as the primary delivery mechanism.
    ///       The bound constructor also sends it as a backup if the client binds the moniker.
    /// </summary>
    [ConcreteService("beyonce")]
    public class beyonce : ClientBoundService
    {
        public override AccessLevel AccessLevel => AccessLevel.None;

        private IItems                     Items                     { get; }
        private INotificationSender        NotificationSender        { get; }
        private SolarSystemDestinyManager  SolarSystemDestinyMgr     { get; }
        private ISessionManager            SessionManager            { get; }
        private IDatabase                  Database                  { get; }
        private ILogger                    Log                       { get; }

        private Ballpark        mBallpark;
        private DestinyManager  mDestinyManager;
        private int             mSolarSystemID;
        private int             mOwnerID;
        private Dictionary<int, List<(int GateID, int SolarSystemID)>> mStargateJumps = new();

        // =====================================================================
        //  GLOBAL / UNBOUND CONSTRUCTOR
        //  Called once at startup. No ballpark here.
        // =====================================================================

        public beyonce(IBoundServiceManager manager, IItems items, INotificationSender notificationSender,
                       SolarSystemDestinyManager solarSystemDestinyMgr, ISessionManager sessionManager, IDatabase database, ILogger logger)
            : base(manager)
        {
            this.Items                 = items;
            this.NotificationSender    = notificationSender;
            this.SolarSystemDestinyMgr = solarSystemDestinyMgr;
            this.SessionManager        = sessionManager;
            this.Database              = database;
            this.Log                   = logger;
        }

        // =====================================================================
        //  BOUND CONSTRUCTOR
        //  Called per client during Moniker.Bind() - this is where we send state.
        // =====================================================================

        internal beyonce(
            IBoundServiceManager       manager,
            Session                    session,
            int                        objectID,
            IItems                     items,
            INotificationSender        notificationSender,
            SolarSystemDestinyManager  solarSystemDestinyMgr,
            ISessionManager            sessionManager,
            IDatabase                  database,
            ILogger                    logger)
            : base(manager, session, objectID)
        {
            this.Items                 = items;
            this.NotificationSender    = notificationSender;
            this.SolarSystemDestinyMgr = solarSystemDestinyMgr;
            this.SessionManager        = sessionManager;
            this.Database              = database;
            this.Log                   = logger;
            this.mSolarSystemID        = objectID;
            this.mOwnerID              = session.CharacterID;

            int shipID    = session.ShipID   ?? 0;
            int stationID = session.StationID;

            // Session StationID is cleared (=0) after undock session change.
            // Retrieve the station ID that ship.Undock() saved before clearing it.
            if (stationID == 0)
                stationID = SolarSystemDestinyMgr.TakeUndockStation(session.CharacterID);

            Log.Information("[beyonce] BIND: solarSystem={SolarSystemID}, char={OwnerID}, ship={ShipID}, station={StationID}", mSolarSystemID, mOwnerID, shipID, stationID);

            // ----------------------------------------------------------
            // Get or create the DestinyManager for this solar system
            // ----------------------------------------------------------
            mDestinyManager = SolarSystemDestinyMgr.GetOrCreate(mSolarSystemID);

            // ----------------------------------------------------------
            // Build the ballpark with all entities the player should see
            // ----------------------------------------------------------
            mBallpark = new Ballpark(mSolarSystemID, mOwnerID);

            // Use LoadItem (cache + DB fallback) to guarantee the ship is loaded even if
            // it was evicted from the cache (e.g. by dogmaIM.OnClientDisconnected or inventory unload).
            ItemEntity shipEntity = shipID != 0 ? Items.LoadItem(shipID) : null;
            if (shipEntity != null)
            {
                Log.Information("[beyonce] Added ship {ShipID} at ({X:F0},{Y:F0},{Z:F0})", shipID, shipEntity.X, shipEntity.Y, shipEntity.Z);
                mBallpark.AddEntity(shipEntity);

                // Register as BubbleEntity in the DestinyManager
                var shipBubble = CreateBubbleEntity(shipEntity, session, true);
                mDestinyManager.RegisterEntity(shipBubble);
            }

            if (stationID != 0 && Items.TryGetItem(stationID, out ItemEntity stationEntity))
            {
                Log.Information("[beyonce] Added station {StationID}", stationID);
                mBallpark.AddEntity(stationEntity);

                // Register station as rigid BubbleEntity (only if not already registered)
                if (!mDestinyManager.TryGetEntity(stationID, out _))
                {
                    var stationBubble = CreateBubbleEntity(stationEntity, session, false);
                    mDestinyManager.RegisterEntity(stationBubble);
                }

                // Set undock velocity on the ship so it launches out of the dock
                if (stationEntity is Station stationItem && mDestinyManager.TryGetEntity(shipID, out var shipBubble))
                {
                    var stationType = stationItem.StationType;
                    double undockSpeed = shipBubble.MaxVelocity;

                    shipBubble.Velocity = new Vector3
                    {
                        X = stationType.DockOrientationX * undockSpeed,
                        Y = stationType.DockOrientationY * undockSpeed,
                        Z = stationType.DockOrientationZ * undockSpeed
                    };
                    shipBubble.SpeedFraction = 1.0;

                    Log.Information("[beyonce] Set undock velocity ({VelX:F0},{VelY:F0},{VelZ:F0}) speed={Speed:F0}",
                        shipBubble.Velocity.X, shipBubble.Velocity.Y, shipBubble.Velocity.Z, undockSpeed);
                }
            }

            // ----------------------------------------------------------
            // Load all celestials (planets, moons, stargates, etc.)
            // ----------------------------------------------------------
            LoadCelestials(mSolarSystemID);

            // ----------------------------------------------------------
            // SEND DoDestinyUpdate IMMEDIATELY
            // The client's Park.__init__() queues events in self.history,
            // and DoPreTick (called each tick after Start()) processes them.
            // No artificial delay needed — the queuing mechanism handles timing.
            // ----------------------------------------------------------
            SendDoDestinyUpdate(session);
        }

        // =====================================================================
        //  MACHO BINDING
        // =====================================================================

        protected override long MachoResolveObject(ServiceCall call, ServiceBindParams bindParams)
        {
            return BoundServiceManager.MachoNet.NodeID;
        }

        protected override BoundService CreateBoundInstance(ServiceCall call, ServiceBindParams bindParams)
        {
            Log.Information("[beyonce] CreateBoundInstance: objectID={ObjectID}, char={CharID}", bindParams.ObjectID, call.Session.CharacterID);
            return new beyonce(BoundServiceManager, call.Session, bindParams.ObjectID,
                               this.Items, this.NotificationSender, this.SolarSystemDestinyMgr, this.SessionManager, this.Database, this.Log);
        }

        // =====================================================================
        //  CLIENT API
        // =====================================================================

        /// <summary>
        /// GetFormations - Called by the client on the GLOBAL service via eve.RemoteSvc('beyonce').
        /// Returns ship formation data. In Apocrypha, formations are unused - return empty tuple.
        /// </summary>
        public PyDataType GetFormations(ServiceCall call)
        {
            Log.Information("[beyonce] GetFormations() called (global service, char={CharID})", call.Session.CharacterID);
            return new PyTuple(0);
        }

        /// <summary>
        /// UpdateStateRequest - Called by the BOUND moniker (remoteBallpark) during
        /// desync recovery (Park.RequestReset). Sends a fresh DoDestinyUpdate.
        /// </summary>
        public PyDataType UpdateStateRequest(ServiceCall call)
        {
            Log.Information("[beyonce] UpdateStateRequest() called, char={CharID}", call.Session.CharacterID);

            EnsureBallpark(call.Session);
            SendDoDestinyUpdate(call.Session);

            return new PyNone();
        }

        /// <summary>
        /// GetInitialState - Alternative method name some client builds use.
        /// </summary>
        public PyDataType GetInitialState(ServiceCall call)
        {
            Log.Information("[beyonce] GetInitialState() -> delegating to UpdateStateRequest");
            return UpdateStateRequest(call);
        }

        // =====================================================================
        //  MOVEMENT COMMANDS
        // =====================================================================

        public PyDataType Stop(ServiceCall call)
        {
            int shipID = call.Session.ShipID ?? 0;
            Log.Information("[beyonce] Stop: ship={ShipID}", shipID);
            mDestinyManager?.CmdStop(shipID);
            return new PyNone();
        }

        /// <summary>
        /// TeardownBallpark - Called when the ballpark is being torn down (docking, jumping, etc).
        /// </summary>
        public PyDataType TeardownBallpark(ServiceCall call)
        {
            Log.Information("[beyonce] TeardownBallpark()");
            int shipID = call.Session.ShipID ?? 0;
            mDestinyManager?.UnregisterEntity(shipID);
            mBallpark = null;
            return new PyNone();
        }

        public PyDataType FollowBall(ServiceCall call, PyInteger ballID, PyInteger range)
        {
            int shipID = call.Session.ShipID ?? 0;
            Log.Information("[beyonce] FollowBall: ship={ShipID}, target={Target}, range={Range}", shipID, ballID?.Value, range?.Value);
            mDestinyManager?.CmdFollowBall(shipID, (int)(ballID?.Value ?? 0), (float)(range?.Value ?? 1000));
            return new PyNone();
        }

        public PyDataType Orbit(ServiceCall call, PyInteger entityID, PyInteger range)
        {
            int shipID = call.Session.ShipID ?? 0;
            Log.Information("[beyonce] Orbit: ship={ShipID}, target={Target}, range={Range}", shipID, entityID?.Value, range?.Value);
            mDestinyManager?.CmdOrbit(shipID, (int)(entityID?.Value ?? 0), (float)(range?.Value ?? 5000));
            return new PyNone();
        }

        public PyDataType AlignTo(ServiceCall call, PyInteger entityID)
        {
            int shipID = call.Session.ShipID ?? 0;
            Log.Information("[beyonce] AlignTo: ship={ShipID}, target={Target}", shipID, entityID?.Value);
            mDestinyManager?.CmdAlignTo(shipID, (int)(entityID?.Value ?? 0));
            return new PyNone();
        }

        public PyDataType GotoDirection(ServiceCall call, PyDecimal x, PyDecimal y, PyDecimal z)
        {
            int shipID = call.Session.ShipID ?? 0;
            Log.Information("[beyonce] GotoDirection: ship={ShipID}, dir=({X},{Y},{Z})", shipID, x?.Value, y?.Value, z?.Value);

            // GotoDirection sends a direction vector from the client.
            // We translate to a distant goto point in that direction.
            double dx = x?.Value ?? 0;
            double dy = y?.Value ?? 0;
            double dz = z?.Value ?? 0;

            if (mDestinyManager != null && mDestinyManager.TryGetEntity(shipID, out var ent))
            {
                var dir = new Vector3 { X = dx, Y = dy, Z = dz }.Normalize();
                double farDist = 1e12; // 1 billion km - effectively infinite
                double gx = ent.Position.X + dir.X * farDist;
                double gy = ent.Position.Y + dir.Y * farDist;
                double gz = ent.Position.Z + dir.Z * farDist;
                mDestinyManager.CmdGotoPoint(shipID, gx, gy, gz);
            }

            return new PyNone();
        }

        public PyDataType SetSpeedFraction(ServiceCall call, PyDecimal fraction)
        {
            int shipID = call.Session.ShipID ?? 0;
            Log.Information("[beyonce] SetSpeedFraction: ship={ShipID}, fraction={Fraction}", shipID, fraction?.Value);
            mDestinyManager?.CmdSetSpeedFraction(shipID, (float)(fraction?.Value ?? 0));
            return new PyNone();
        }

        public PyDataType WarpToStuff(ServiceCall call, PyString type, PyInteger itemID)
        {
            int shipID = call.Session.ShipID ?? 0;
            Log.Information("[beyonce] WarpToStuff: ship={ShipID}, type={WarpType}, item={ItemID}", shipID, type?.Value, itemID?.Value);

            int targetID = (int)(itemID?.Value ?? 0);
            if (targetID != 0 && Items.TryGetItem(targetID, out ItemEntity target))
            {
                double tx = target.X ?? 0;
                double ty = target.Y ?? 0;
                double tz = target.Z ?? 0;
                mDestinyManager?.CmdWarpTo(shipID, tx, ty, tz);
            }

            return new PyNone();
        }

        public PyDataType WarpToStuffAutopilot(ServiceCall call, PyInteger itemID)
        {
            int shipID = call.Session.ShipID ?? 0;
            Log.Information("[beyonce] WarpToStuffAutopilot: ship={ShipID}, item={ItemID}", shipID, itemID?.Value);

            int targetID = (int)(itemID?.Value ?? 0);
            if (targetID != 0 && Items.TryGetItem(targetID, out ItemEntity target))
            {
                double tx = target.X ?? 0;
                double ty = target.Y ?? 0;
                double tz = target.Z ?? 0;
                mDestinyManager?.CmdWarpTo(shipID, tx, ty, tz);
            }

            return new PyNone();
        }

        public PyDataType Dock(ServiceCall call, PyInteger stationID)
        {
            int charID = call.Session.CharacterID;
            int shipID = call.Session.ShipID ?? 0;
            int targetStation = (int)(stationID?.Value ?? 0);

            Log.Information("[beyonce] Dock: char={CharID}, ship={ShipID}, station={StationID}", charID, shipID, targetStation);

            if (targetStation == 0)
                return new PyNone();

            var station = Items.GetStaticStation(targetStation);
            if (station == null)
            {
                Log.Warning("[beyonce] Dock: station {StationID} not found", targetStation);
                return new PyNone();
            }

            // Unregister ship from DestinyManager
            mDestinyManager?.UnregisterEntity(shipID);

            // Move ship to the new station in DB (same pattern as /move GM command)
            // Use LoadItem to guarantee the ship is loaded even if evicted from cache.
            ItemEntity shipEntity = shipID != 0 ? Items.LoadItem(shipID) : null;
            if (shipEntity != null)
            {
                shipEntity.LocationID = targetStation;
                shipEntity.Flag = Flags.Hangar;
                shipEntity.Persist();
                Log.Information("[beyonce] Dock: Moved ship {ShipID} to station {StationID} in DB", shipID, targetStation);
            }

            // Update character location in chrInformation for login persistence
            Database.Prepare(
                "UPDATE chrInformation " +
                "SET stationID = @stationID, solarSystemID = @solarSystemID, " +
                "    constellationID = @constellationID, regionID = @regionID " +
                "WHERE characterID = @characterID",
                new Dictionary<string, object>
                {
                    {"@characterID", charID},
                    {"@stationID", targetStation},
                    {"@solarSystemID", station.SolarSystemID},
                    {"@constellationID", station.ConstellationID},
                    {"@regionID", station.RegionID}
                }
            );
            Log.Information("[beyonce] Dock: Updated chrInformation for char {CharID}", charID);

            // Session change: enter station (reverse of ship.Undock)
            var delta = new Session();
            delta[Session.STATION_ID]       = (PyInteger)targetStation;
            delta[Session.LOCATION_ID]      = (PyInteger)targetStation;
            delta[Session.SOLAR_SYSTEM_ID]  = new PyNone();
            delta[Session.SOLAR_SYSTEM_ID2] = (PyInteger)station.SolarSystemID;
            delta[Session.CONSTELLATION_ID] = (PyInteger)station.ConstellationID;
            delta[Session.REGION_ID]        = (PyInteger)station.RegionID;

            Log.Information("[beyonce] Dock: performing session update for char {CharID} -> station {StationID}", charID, targetStation);
            SessionManager.PerformSessionUpdate(Session.CHAR_ID, charID, delta);
            Log.Information("[beyonce] Dock: session update completed");

            return new PyNone();
        }

        public PyDataType StargateJump(ServiceCall call, PyInteger fromID, PyInteger toID)
        {
            int charID = call.Session.CharacterID;
            int shipID = call.Session.ShipID ?? 0;
            int destGateID = (int)(toID?.Value ?? 0);

            Log.Information("[beyonce] StargateJump: char={CharID}, ship={ShipID}, from={FromID}, to={ToID}", charID, shipID, fromID?.Value, destGateID);

            if (destGateID == 0)
                return new PyNone();

            // Look up destination gate's solar system, position, constellation, and region
            var destInfo = GetStargateDestinationInfo(destGateID);
            if (destInfo == null)
            {
                Log.Warning("[beyonce] StargateJump: could not find destination info for gate {GateID}", destGateID);
                return new PyNone();
            }

            Log.Information("[beyonce] StargateJump: destination system={SolarSystemID}, constellation={ConstellationID}, region={RegionID}, pos=({X:F0},{Y:F0},{Z:F0})",
                destInfo.Value.SolarSystemID, destInfo.Value.ConstellationID, destInfo.Value.RegionID,
                destInfo.Value.X, destInfo.Value.Y, destInfo.Value.Z);

            // Unregister ship from current DestinyManager
            mDestinyManager?.UnregisterEntity(shipID);

            // Update ship position to near the destination gate and persist
            // Use LoadItem to guarantee the ship is loaded even if evicted from cache.
            ItemEntity shipEntity = shipID != 0 ? Items.LoadItem(shipID) : null;
            if (shipEntity != null)
            {
                // Place ship 15km from the gate (offset along X to avoid overlap)
                shipEntity.X = destInfo.Value.X + 15000;
                shipEntity.Y = destInfo.Value.Y;
                shipEntity.Z = destInfo.Value.Z;
                shipEntity.LocationID = destInfo.Value.SolarSystemID;
                shipEntity.Persist();
                Log.Information("[beyonce] StargateJump: Ship {ShipID} persisted at ({X:F0},{Y:F0},{Z:F0}), locationID={LocationID}",
                    shipID, shipEntity.X, shipEntity.Y, shipEntity.Z, destInfo.Value.SolarSystemID);
            }

            // Session change: transition to new solar system
            var delta = new Session();
            delta[Session.STATION_ID]       = new PyNone();
            delta[Session.LOCATION_ID]      = (PyInteger)destInfo.Value.SolarSystemID;
            delta[Session.SOLAR_SYSTEM_ID]  = (PyInteger)destInfo.Value.SolarSystemID;
            delta[Session.SOLAR_SYSTEM_ID2] = (PyInteger)destInfo.Value.SolarSystemID;
            delta[Session.CONSTELLATION_ID] = (PyInteger)destInfo.Value.ConstellationID;
            delta[Session.REGION_ID]        = (PyInteger)destInfo.Value.RegionID;

            Log.Information("[beyonce] StargateJump: performing session update for char {CharID} -> system {SystemID}", charID, destInfo.Value.SolarSystemID);
            SessionManager.PerformSessionUpdate(Session.CHAR_ID, charID, delta);
            Log.Information("[beyonce] StargateJump: session update completed");

            return new PyNone();
        }

        private struct StargateDestination
        {
            public int SolarSystemID;
            public double X, Y, Z;
            public int ConstellationID;
            public int RegionID;
        }

        private StargateDestination? GetStargateDestinationInfo(int gateID)
        {
            try
            {
                DbDataReader reader = Database.Select(
                    "SELECT md.solarSystemID, md.x, md.y, md.z, ms.constellationID, ms.regionID " +
                    "FROM mapDenormalize md " +
                    "JOIN mapSolarSystems ms ON ms.solarSystemID = md.solarSystemID " +
                    "WHERE md.itemID = @itemID",
                    new Dictionary<string, object> { { "@itemID", gateID } }
                );

                using (reader)
                {
                    if (reader.Read())
                    {
                        return new StargateDestination
                        {
                            SolarSystemID   = reader.GetInt32(0),
                            X               = reader.GetDouble(1),
                            Y               = reader.GetDouble(2),
                            Z               = reader.GetDouble(3),
                            ConstellationID = reader.GetInt32(4),
                            RegionID        = reader.GetInt32(5)
                        };
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[beyonce] Failed to get destination info for gate {GateID}: {Message}", gateID, ex.Message);
            }

            return null;
        }

        // =====================================================================
        //  DoDestinyUpdate NOTIFICATION
        // =====================================================================

        private void SendDoDestinyUpdate(Session session)
        {
            int charID = session.CharacterID;
            int shipID = session.ShipID ?? 0;

            Log.Information("[beyonce] SendDoDestinyUpdate: char={CharID}, ship={ShipID}, system={SolarSystemID}", charID, shipID, mSolarSystemID);

            if (mBallpark == null || mBallpark.Entities.Count == 0)
            {
                Log.Warning("[beyonce] No entities in ballpark, cannot send state");
                return;
            }

            try
            {
                // Build the state event list: [(timestamp, ('SetState', (bagKeyVal,)))]
                PyDataType stateEvents = BuildSnapshot(mSolarSystemID, shipID, session);

                // Wrap as DoDestinyUpdate args: (state_list, waitForBubble, dogmaMessages)
                PyTuple notificationData = new PyTuple(3)
                {
                    [0] = stateEvents,
                    [1] = new PyBool(false),  // waitForBubble
                    [2] = new PyList()        // dogmaMessages
                };

                // Send via solarsystemid2 broadcast (standard EVE routing)
                NotificationSender.SendNotification(
                    "DoDestinyUpdate",
                    "solarsystemid2",
                    mSolarSystemID,
                    notificationData
                );
                Log.Information("[beyonce] DoDestinyUpdate sent via solarsystemid2 broadcast to system {SystemID}", mSolarSystemID);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[beyonce] ERROR sending DoDestinyUpdate: {Message}", ex.Message);
            }
        }

        // =====================================================================
        //  SNAPSHOT BUILDER
        // =====================================================================

        private PyDataType BuildSnapshot(int solarSystemID, int shipID, Session sess)
        {
            Log.Information("[beyonce] BuildSnapshot: system={SolarSystemID}, ship={ShipID}", solarSystemID, shipID);

            // Compute stamp once and share between destiny binary and event tuple.
            // CRITICAL: stamp must be > 0, otherwise the client's FlushState() silently
            // drops the SetState event (entryTime > newestOldStateTime fails when both are 0).
            long eveEpoch = new DateTime(2003, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
            int stamp = (int)((DateTime.UtcNow.Ticks - eveEpoch) / 10000000 % int.MaxValue);

            byte[] destinyData = BuildDestinyBinary(solarSystemID, shipID, sess, stamp);
            Log.Information("[beyonce] Destiny binary: {ByteCount} bytes", destinyData.Length);

            var bagDict = new PyDictionary();

            bagDict["aggressors"] = new PyDictionary();
            bagDict["droneState"] = BuildEmptyDroneState();
            bagDict["solItem"]    = BuildSolItem(solarSystemID);
            bagDict["state"]      = new PyBuffer(destinyData);
            bagDict["ego"]        = new PyInteger(shipID);

            var slims = new PyList();

            // Build slim items for all entities in the ballpark
            foreach (var kvpSlim in mBallpark.Entities)
            {
                var ent = kvpSlim.Value;
                bool isShip = (ent.ID == shipID);

                var slim = BuildSlimItem(
                    ent.ID,
                    ent.Type.ID,
                    ent.Type.Group.ID,
                    ent.Type.Group.Category.ID,
                    ent.Name ?? ent.Type?.Name ?? "Unknown",
                    isShip ? sess.CharacterID : ent.OwnerID,
                    solarSystemID,
                    isShip ? sess.CorporationID : 0,
                    0,
                    isShip ? sess.CharacterID : 0
                );

                // Ships need a 'modules' field or the client crashes in FitHardpoints2
                if (isShip)
                {
                    var slimDict = (PyDictionary)slim.Arguments;
                    var modulesList = new PyList();

                    // Load the ship to get its fitted modules for 3D hardpoint rendering
                    try
                    {
                        var ship = Items.LoadItem<Ship>(ent.ID);
                        if (ship != null)
                        {
                            foreach (var (_, module) in ship.Items)
                            {
                                if (module.IsInModuleSlot() || module.IsInRigSlot())
                                {
                                    var modEntry = new PyTuple(2)
                                    {
                                        [0] = new PyInteger(module.Type.ID),
                                        [1] = new PyInteger((int)module.Flag)
                                    };
                                    modulesList.Add(modEntry);
                                }
                            }

                            Log.Information("[beyonce] Ship {ShipID} has {ModuleCount} fitted modules", ent.ID, modulesList.Count);
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Warning(ex, "[beyonce] Failed to load modules for ship {ShipID}: {Message}", ent.ID, ex.Message);
                    }

                    slimDict["modules"] = modulesList;
                }

                // Stargates need destination gate IDs in 'jumps' for the right-click menu
                int groupID = ent.Type?.Group?.ID ?? 0;
                if (groupID == (int)GroupID.Stargate && mStargateJumps.TryGetValue(ent.ID, out var jumpDests))
                {
                    var slimDict = (PyDictionary)slim.Arguments;
                    var jumpsList = new PyList();
                    foreach (var jump in jumpDests)
                        jumpsList.Add(new PyObjectData("util.KeyVal", new PyDictionary
                        {
                            ["locationID"]    = new PyInteger(jump.SolarSystemID),
                            ["toCelestialID"] = new PyInteger(jump.GateID)
                        }));
                    slimDict["jumps"] = jumpsList;
                }

                slims.Add(slim);
                Log.Information("[beyonce] Added slim: id={EntityID}, type={TypeName}, isShip={IsShip}", ent.ID, ent.Type?.Name, isShip);
            }

            bagDict["slims"] = slims;

            bagDict["damageState"]     = BuildDamageState(shipID, stamp);
            bagDict["effectStates"]    = new PyList();
            bagDict["allianceBridges"] = new PyList();

            Log.Information("[beyonce] Snapshot: {SlimCount} slims", slims.Count);

            var bagKeyVal     = new PyObjectData("util.KeyVal", bagDict);
            var stateCallArgs = new PyTuple(1) { [0] = bagKeyVal };
            var innerCall     = new PyTuple(2) { [0] = new PyString("SetState"), [1] = stateCallArgs };
            var eventTuple    = new PyTuple(2) { [0] = new PyInteger(stamp), [1] = innerCall };
            var events        = new PyList();
            events.Add(eventTuple);

            return events;
        }

        // =====================================================================
        //  DESTINY BINARY ENCODER
        // =====================================================================

        private byte[] BuildDestinyBinary(int solarSystemID, int egoShipID, Session sess, int stamp)
        {
            if (mBallpark == null || mBallpark.Entities.Count == 0)
                return Array.Empty<byte>();

            var balls = new List<Ball>();

            foreach (var kvp in mBallpark.Entities)
            {
                ItemEntity ent = kvp.Value;
                bool isEgo = (ent.ID == egoShipID);

                // If we have a BubbleEntity with live position, use it
                double x, y, z;
                BubbleEntity bubbleEnt = null;
                if (mDestinyManager != null && mDestinyManager.TryGetEntity(ent.ID, out bubbleEnt))
                {
                    x = bubbleEnt.Position.X;
                    y = bubbleEnt.Position.Y;
                    z = bubbleEnt.Position.Z;
                }
                else
                {
                    x = ent.X ?? 0;
                    y = ent.Y ?? 0;
                    z = ent.Z ?? 0;
                }

                BallFlag flags;
                BallMode mode;

                if (isEgo)
                {
                    flags = BallFlag.IsFree | BallFlag.IsMassive | BallFlag.IsInteractive;
                    mode  = BallMode.Stop;
                }
                else
                {
                    flags = BallFlag.IsGlobal | BallFlag.IsMassive | BallFlag.IsInteractive;
                    mode  = BallMode.Rigid;
                }

                var header = new BallHeader
                {
                    ItemId   = ent.ID,
                    Mode     = mode,
                    Radius   = isEgo ? 50.0
                               : (bubbleEnt?.Radius ?? ent.Type?.Radius ?? 5000.0),
                    Location = new Vector3 { X = x, Y = y, Z = z },
                    Flags    = flags
                };

                Ball ball = new Ball
                {
                    Header      = header,
                    FormationId = 0xFF
                };

                if (mode != BallMode.Rigid)
                {
                    ball.ExtraHeader = new ExtraBallHeader
                    {
                        Mass          = 1000000.0,
                        CloakMode     = CloakMode.None,
                        Harmonic      = 0xFFFFFFFFFFFFFFFF,
                        CorporationId = isEgo ? sess.CorporationID : 0,
                        AllianceId    = 0
                    };

                    if (flags.HasFlag(BallFlag.IsFree))
                    {
                        ball.Data = new BallData
                        {
                            MaxVelocity   = 200.0,
                            Velocity      = new Vector3 { X = 0, Y = 0, Z = 0 },
                            UnknownVec    = default,
                            Agility       = 1.0,
                            SpeedFraction = 0.0
                        };
                    }
                }

                balls.Add(ball);
                Log.Information("[beyonce] Ball: id={BallID}, ego={IsEgo}, mode={Mode}, flags={Flags}, pos=({X:F0},{Y:F0},{Z:F0})", ent.ID, isEgo, mode, flags, x, y, z);
            }

            byte[] result = DestinyBinaryEncoder.BuildFullState(balls, stamp, 0);
            Log.Information("[beyonce] Encoded {BallCount} balls -> {ByteCount} bytes", balls.Count, result.Length);

            return result;
        }

        // =====================================================================
        //  HELPERS
        // =====================================================================

        /// <summary>
        /// Create a BubbleEntity from an ItemEntity for the DestinyManager.
        /// </summary>
        private static BubbleEntity CreateBubbleEntity(ItemEntity entity, Session session, bool isPlayerShip)
        {
            double x = entity.X ?? 0;
            double y = entity.Y ?? 0;
            double z = entity.Z ?? 0;

            BallFlag flags;
            BallMode mode;

            if (isPlayerShip)
            {
                flags = BallFlag.IsFree | BallFlag.IsMassive | BallFlag.IsInteractive;
                mode  = BallMode.Stop;
            }
            else
            {
                flags = BallFlag.IsGlobal | BallFlag.IsMassive | BallFlag.IsInteractive;
                mode  = BallMode.Rigid;
            }

            return new BubbleEntity
            {
                ItemID        = entity.ID,
                TypeID        = entity.Type?.ID ?? 0,
                GroupID       = entity.Type?.Group?.ID ?? 0,
                CategoryID    = entity.Type?.Group?.Category?.ID ?? 0,
                Name          = entity.Name ?? entity.Type?.Name ?? "Unknown",
                OwnerID       = isPlayerShip ? session.CharacterID : entity.OwnerID,
                CorporationID = isPlayerShip ? session.CorporationID : 0,
                AllianceID    = 0,
                CharacterID   = isPlayerShip ? session.CharacterID : 0,
                Position      = new Vector3 { X = x, Y = y, Z = z },
                Velocity      = default,
                Mode          = mode,
                Flags         = flags,
                Radius        = isPlayerShip ? 50.0 : (entity.Type?.Radius ?? 5000.0),
                Mass          = 1000000.0,
                MaxVelocity   = isPlayerShip ? 200.0 : 0.0,
                SpeedFraction = 0.0,
                Agility       = 1.0
            };
        }

        // Groups that represent celestial objects visible in the overview
        private static readonly HashSet<int> CelestialGroups = new HashSet<int>
        {
            (int)GroupID.Sun,
            (int)GroupID.Planet,
            (int)GroupID.Moon,
            (int)GroupID.AsteroidBelt,
            (int)GroupID.Stargate,
            (int)GroupID.Station,
        };

        private void LoadCelestials(int solarSystemID)
        {
            try
            {
                var solarSystem = Items.GetStaticSolarSystem(solarSystemID);
                var allItems = Items.LoadAllItemsLocatedAt(solarSystem);

                Log.Information("[beyonce] LoadCelestials: {TotalItems} total items found in solar system {SystemID}", allItems.Count, solarSystemID);

                // Load per-item radii from mapDenormalize (planets, moons, etc. each have unique radii)
                var celestialRadii = new Dictionary<int, double>();
                DbDataReader radiusReader = Database.Select(
                    "SELECT itemID, radius FROM mapDenormalize WHERE solarSystemID = @solarSystemID AND radius IS NOT NULL",
                    new Dictionary<string, object> { { "@solarSystemID", solarSystemID } }
                );
                using (radiusReader)
                {
                    while (radiusReader.Read())
                        celestialRadii[radiusReader.GetInt32(0)] = radiusReader.GetDouble(1);
                }

                int count = 0;
                foreach (var kvp in allItems)
                {
                    ItemEntity ent = kvp.Value;
                    int groupID = ent.Type?.Group?.ID ?? 0;

                    if (!CelestialGroups.Contains(groupID))
                        continue;

                    // Skip entities already in the ballpark (e.g. undock station)
                    if (mBallpark.Entities.ContainsKey(ent.ID))
                        continue;

                    double radius = celestialRadii.TryGetValue(ent.ID, out double mdRadius)
                        ? mdRadius
                        : (ent.Type?.Radius ?? 5000.0);
                    int typeID = ent.Type?.ID ?? 0;
                    string groupName = ent.Type?.Group?.Name ?? "???";

                    Log.Information(
                        "[beyonce]   Celestial: itemID={ItemID} typeID={TypeID} group={GroupName}({GroupID}) " +
                        "name=\"{Name}\" radius={Radius:F0}m pos=({X:F0}, {Y:F0}, {Z:F0})",
                        ent.ID, typeID, groupName, groupID,
                        ent.Name ?? ent.Type?.Name ?? "Unknown",
                        radius, ent.X ?? 0, ent.Y ?? 0, ent.Z ?? 0);

                    mBallpark.AddEntity(ent);

                    if (!mDestinyManager.TryGetEntity(ent.ID, out _))
                    {
                        var bubble = new BubbleEntity
                        {
                            ItemID        = ent.ID,
                            TypeID        = typeID,
                            GroupID       = groupID,
                            CategoryID    = ent.Type?.Group?.Category?.ID ?? 0,
                            Name          = ent.Name ?? ent.Type?.Name ?? "Unknown",
                            OwnerID       = ent.OwnerID,
                            CorporationID = 0,
                            AllianceID    = 0,
                            CharacterID   = 0,
                            Position      = new Vector3 { X = ent.X ?? 0, Y = ent.Y ?? 0, Z = ent.Z ?? 0 },
                            Velocity      = default,
                            Mode          = BallMode.Rigid,
                            Flags         = BallFlag.IsGlobal | BallFlag.IsMassive | BallFlag.IsInteractive,
                            Radius        = radius,
                            Mass          = 1000000.0,
                            MaxVelocity   = 0.0,
                            SpeedFraction = 0.0,
                            Agility       = 1.0
                        };
                        mDestinyManager.RegisterEntity(bubble);
                    }

                    count++;
                }

                Log.Information("[beyonce] Loaded {Count} celestials for system {SystemID}", count, solarSystemID);

                // Load stargate jump destinations for this solar system
                LoadStargateJumps(solarSystemID);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[beyonce] Failed to load celestials for system {SystemID}: {Message}", solarSystemID, ex.Message);
            }
        }

        private void LoadStargateJumps(int solarSystemID)
        {
            try
            {
                mStargateJumps.Clear();

                DbDataReader reader = Database.Select(
                    "SELECT mj.stargateID, mj.celestialID, md2.solarSystemID " +
                    "FROM mapJumps mj " +
                    "INNER JOIN mapDenormalize md ON md.itemID = mj.stargateID " +
                    "INNER JOIN mapDenormalize md2 ON md2.itemID = mj.celestialID " +
                    "WHERE md.solarSystemID = @solarSystemID AND md.groupID = 10",
                    new Dictionary<string, object> { { "@solarSystemID", solarSystemID } }
                );

                using (reader)
                {
                    while (reader.Read())
                    {
                        int stargateID      = reader.GetInt32(0);
                        int destGateID      = reader.GetInt32(1);
                        int destSolarSystem = reader.GetInt32(2);

                        if (!mStargateJumps.TryGetValue(stargateID, out var dests))
                        {
                            dests = new List<(int, int)>();
                            mStargateJumps[stargateID] = dests;
                        }
                        dests.Add((destGateID, destSolarSystem));
                    }
                }

                Log.Information("[beyonce] Loaded stargate jumps: {Count} gates with destinations in system {SystemID}", mStargateJumps.Count, solarSystemID);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[beyonce] Failed to load stargate jumps for system {SystemID}: {Message}", solarSystemID, ex.Message);
            }
        }

        private void EnsureBallpark(Session session)
        {
            if (mBallpark != null)
                return;

            int solarSystemID = session.SolarSystemID ?? mSolarSystemID;
            int ownerID       = session.CharacterID;
            int shipID        = session.ShipID ?? 0;
            int stationID     = session.StationID;

            mBallpark = new Ballpark(solarSystemID, ownerID);
            mDestinyManager = SolarSystemDestinyMgr.GetOrCreate(solarSystemID);

            // Use LoadItem to guarantee the ship is loaded even if evicted from cache.
            ItemEntity shipEntity = shipID != 0 ? Items.LoadItem(shipID) : null;
            if (shipEntity != null)
            {
                mBallpark.AddEntity(shipEntity);
                if (!mDestinyManager.TryGetEntity(shipID, out _))
                {
                    var shipBubble = CreateBubbleEntity(shipEntity, session, true);
                    mDestinyManager.RegisterEntity(shipBubble);
                }
            }

            if (stationID != 0 && Items.TryGetItem(stationID, out ItemEntity stationEntity))
            {
                mBallpark.AddEntity(stationEntity);
                if (!mDestinyManager.TryGetEntity(stationID, out _))
                {
                    var stationBubble = CreateBubbleEntity(stationEntity, session, false);
                    mDestinyManager.RegisterEntity(stationBubble);
                }
            }

            LoadCelestials(solarSystemID);
        }

        private static PyObjectData BuildSolItem(int solID)
        {
            var d = new PyDictionary
            {
                ["itemID"]          = new PyInteger(solID),
                ["typeID"]          = new PyInteger(5),
                ["groupID"]         = new PyInteger(5),
                ["ownerID"]         = new PyInteger(1),
                ["locationID"]      = new PyInteger(0),
                ["x"]               = new PyInteger(0),
                ["y"]               = new PyInteger(0),
                ["z"]               = new PyInteger(0),
                ["categoryID"]      = new PyInteger(2),
                ["name"]            = new PyString("Solar System"),
                ["corpID"]          = new PyInteger(0),
                ["allianceID"]      = new PyInteger(0),
                ["charID"]          = new PyInteger(0),
                ["dunObjectID"]     = new PyNone(),
                ["jumps"]           = new PyList(),
                ["securityStatus"]  = new PyDecimal(0.0),
                ["orbitalVelocity"] = new PyDecimal(0.0),
                ["warFactionID"]    = new PyNone()
            };

            return new PyObjectData("util.KeyVal", d);
        }

        private static PyObjectData BuildSlimItem(
            int itemID, int typeID, int groupID, int categoryID, string name,
            int ownerID, int locationID, int corpID, int allianceID, int charID)
        {
            var d = new PyDictionary
            {
                ["itemID"]          = new PyInteger(itemID),
                ["typeID"]          = new PyInteger(typeID),
                ["groupID"]         = new PyInteger(groupID),
                ["ownerID"]         = new PyInteger(ownerID),
                ["locationID"]      = new PyInteger(locationID),
                ["categoryID"]      = new PyInteger(categoryID),
                ["name"]            = new PyString(name),
                ["corpID"]          = new PyInteger(corpID),
                ["allianceID"]      = new PyInteger(allianceID),
                ["charID"]          = new PyInteger(charID),
                ["dunObjectID"]     = new PyNone(),
                ["jumps"]           = new PyList(),
                ["securityStatus"]  = new PyDecimal(0.0),
                ["orbitalVelocity"] = new PyDecimal(0.0),
                ["warFactionID"]    = new PyNone()
            };

            return new PyObjectData("util.KeyVal", d);
        }

        private static PyDictionary BuildDamageState(int shipID, int stamp)
        {
            if (shipID == 0)
                return new PyDictionary();

            // {shipID: ((shieldFraction, armorFraction, hullFraction), timestamp, isRepairing)}
            var hpTuple = new PyTuple(3)
            {
                [0] = new PyDecimal(1.0),  // shield
                [1] = new PyDecimal(1.0),  // armor
                [2] = new PyDecimal(1.0)   // hull
            };

            var entry = new PyTuple(3)
            {
                [0] = hpTuple,
                [1] = new PyInteger(stamp),
                [2] = new PyBool(false)
            };

            return new PyDictionary
            {
                [new PyInteger(shipID)] = entry
            };
        }

        private static PyObjectData BuildEmptyDroneState()
        {
            return new PyObjectData(
                "util.Rowset",
                new PyDictionary
                {
                    ["header"]   = new PyList
                    {
                        new PyString("droneID"),
                        new PyString("ownerID"),
                        new PyString("controllerID"),
                        new PyString("activityState"),
                        new PyString("typeID"),
                        new PyString("controllerOwnerID"),
                        new PyString("targetID")
                    },
                    ["RowClass"] = new PyString("util.Row"),
                    ["lines"]    = new PyList()
                }
            );
        }

        protected override void OnClientDisconnected()
        {
            Log.Information("[beyonce] Client disconnected");
            // Unregister ship from destiny when client disconnects
            int shipID = 0;
            if (mBallpark != null)
            {
                mDestinyManager?.UnregisterEntity(shipID);
            }
        }
    }
}
