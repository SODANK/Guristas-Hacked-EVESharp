using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using EVESharp.Destiny;

namespace EVESharp.Node.Services.Space
{
    /// <summary>
    /// Per-solar-system physics tick loop. Processes movement commands and
    /// broadcasts incremental destiny updates to bubble occupants.
    /// </summary>
    public class DestinyManager : IDisposable
    {
        // EVE physics constants
        private const double TIC_DURATION    = 1.0;   // seconds per tick
        private const double SPACE_FRICTION  = 1.0e6;
        private const double AU_IN_METERS    = 1.496e11;
        private const double WARP_SPEED_AUS  = 3.0;                        // AU/s - sent to client
        private const double WARP_SPEED      = WARP_SPEED_AUS * AU_IN_METERS; // m/s - server physics
        private const double ARRIVE_DIST     = 15000.0; // stop distance for goto/follow
        private const double ORBIT_TOLERANCE = 500.0;

        public int SolarSystemID { get; }
        public BubbleManager BubbleManager { get; }

        private readonly DestinyBroadcaster               mBroadcaster;
        private readonly ConcurrentDictionary<int, BubbleEntity> mEntities = new ConcurrentDictionary<int, BubbleEntity>();
        private readonly ConcurrentQueue<Action>           mPendingCommands = new ConcurrentQueue<Action>();
        private readonly Timer                             mTickTimer;
        private          bool                              mDisposed;

        public DestinyManager(int solarSystemID, DestinyBroadcaster broadcaster)
        {
            SolarSystemID = solarSystemID;
            BubbleManager = new BubbleManager();
            mBroadcaster  = broadcaster;

            // Start tick loop at 1 second intervals
            mTickTimer = new Timer(Tick, null, 1000, 1000);
            Console.WriteLine($"[DestinyManager] Started for system {solarSystemID}");
        }

        // =====================================================================
        //  ENTITY MANAGEMENT
        // =====================================================================

        public void RegisterEntity(BubbleEntity entity)
        {
            mEntities[entity.ItemID] = entity;
            BubbleManager.AddEntity(entity);
            Console.WriteLine($"[DestinyManager] Registered entity {entity.ItemID} ({entity.Name}) in system {SolarSystemID}");
        }

        public void UnregisterEntity(int itemID)
        {
            mEntities.TryRemove(itemID, out _);
            BubbleManager.RemoveEntity(itemID);
        }

        public bool TryGetEntity(int itemID, out BubbleEntity entity)
        {
            return mEntities.TryGetValue(itemID, out entity);
        }

        /// <summary>
        /// Returns all registered entities. Used by GM commands like /unspawn.
        /// </summary>
        public IEnumerable<BubbleEntity> GetEntities()
        {
            return mEntities.Values;
        }

        // =====================================================================
        //  COMMAND METHODS (called from beyonce service thread)
        //  These enqueue commands to be processed on the next tick.
        // =====================================================================

        public void CmdStop(int shipID)
        {
            mPendingCommands.Enqueue(() =>
            {
                if (!mEntities.TryGetValue(shipID, out var ent)) return;

                ent.Mode          = BallMode.Stop;
                ent.Velocity      = default;
                ent.SpeedFraction = 0;

                var bubble = BubbleManager.GetBubbleForEntity(shipID);
                if (bubble != null)
                {
                    var events = DestinyEventBuilder.BuildSetSpeedFraction(shipID, 0);
                    mBroadcaster.BroadcastToSystem(SolarSystemID, events);
                }

                Console.WriteLine($"[DestinyManager] Stop: entity {shipID}");
            });
        }

        public void CmdGotoPoint(int shipID, double x, double y, double z)
        {
            mPendingCommands.Enqueue(() =>
            {
                if (!mEntities.TryGetValue(shipID, out var ent)) return;

                ent.Mode       = BallMode.Goto;
                ent.GotoTarget = new Vector3 { X = x, Y = y, Z = z };
                if (ent.SpeedFraction <= 0) ent.SpeedFraction = 1.0;

                var bubble = BubbleManager.GetBubbleForEntity(shipID);
                if (bubble != null)
                {
                    var events = DestinyEventBuilder.BuildGotoPoint(shipID, x, y, z);
                    mBroadcaster.BroadcastToSystem(SolarSystemID, events);
                }

                Console.WriteLine($"[DestinyManager] GotoPoint: entity {shipID} → ({x:F0},{y:F0},{z:F0})");
            });
        }

        public void CmdFollowBall(int shipID, int targetID, float range)
        {
            mPendingCommands.Enqueue(() =>
            {
                if (!mEntities.TryGetValue(shipID, out var ent)) return;

                ent.Mode           = BallMode.Follow;
                ent.FollowTargetID = targetID;
                ent.FollowRange    = range;
                if (ent.SpeedFraction <= 0) ent.SpeedFraction = 1.0;

                var bubble = BubbleManager.GetBubbleForEntity(shipID);
                if (bubble != null)
                {
                    var events = DestinyEventBuilder.BuildFollowBall(shipID, targetID, range);
                    mBroadcaster.BroadcastToSystem(SolarSystemID, events);
                }

                Console.WriteLine($"[DestinyManager] FollowBall: entity {shipID} → target {targetID}, range {range}");
            });
        }

        public void CmdOrbit(int shipID, int targetID, float range)
        {
            mPendingCommands.Enqueue(() =>
            {
                if (!mEntities.TryGetValue(shipID, out var ent)) return;

                ent.Mode           = BallMode.Orbit;
                ent.FollowTargetID = targetID;
                ent.FollowRange    = range;
                if (ent.SpeedFraction <= 0) ent.SpeedFraction = 1.0;

                var bubble = BubbleManager.GetBubbleForEntity(shipID);
                if (bubble != null)
                {
                    var events = DestinyEventBuilder.BuildOrbit(shipID, targetID, range);
                    mBroadcaster.BroadcastToSystem(SolarSystemID, events);
                }

                Console.WriteLine($"[DestinyManager] Orbit: entity {shipID} → target {targetID}, range {range}");
            });
        }

        public void CmdSetSpeedFraction(int shipID, float fraction)
        {
            mPendingCommands.Enqueue(() =>
            {
                if (!mEntities.TryGetValue(shipID, out var ent)) return;

                ent.SpeedFraction = Math.Clamp(fraction, 0.0, 1.0);

                var bubble = BubbleManager.GetBubbleForEntity(shipID);
                if (bubble != null)
                {
                    var events = DestinyEventBuilder.BuildSetSpeedFraction(shipID, ent.SpeedFraction);
                    mBroadcaster.BroadcastToSystem(SolarSystemID, events);
                }

                Console.WriteLine($"[DestinyManager] SetSpeedFraction: entity {shipID} = {ent.SpeedFraction}");
            });
        }

        public void CmdAlignTo(int shipID, int targetID)
        {
            // AlignTo is Follow with 0 range (approach target direction)
            CmdFollowBall(shipID, targetID, 0);
        }

        public void CmdWarpTo(int shipID, double x, double y, double z)
        {
            mPendingCommands.Enqueue(() =>
            {
                if (!mEntities.TryGetValue(shipID, out var ent)) return;

                long eveEpoch = new DateTime(2003, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
                int stamp = (int)((DateTime.UtcNow.Ticks - eveEpoch) / 10000000 % int.MaxValue);

                ent.Mode            = BallMode.Warp;
                ent.WarpTarget      = new Vector3 { X = x, Y = y, Z = z };
                ent.WarpEffectStamp = stamp;
                ent.SpeedFraction   = 1.0;

                var bubble = BubbleManager.GetBubbleForEntity(shipID);
                if (bubble != null)
                {
                    // Send GotoPoint first (starts alignment + acceleration toward destination),
                    // then WarpTo (engages warp once the ball reaches 75% max velocity while aligned).
                    // The client's native destiny code requires the ball to be in motion for warp to commit.
                    var events = DestinyEventBuilder.BuildGotoPoint(shipID, x, y, z);
                    var warpEvents = DestinyEventBuilder.BuildWarpTo(shipID, x, y, z, WARP_SPEED_AUS, stamp);
                    for (int i = 0; i < warpEvents.Count; i++)
                        events.Add(warpEvents[i]);

                    mBroadcaster.BroadcastToSystem(SolarSystemID, events);
                }

                Console.WriteLine($"[DestinyManager] WarpTo: entity {shipID} → ({x:F0},{y:F0},{z:F0})");
            });
        }

        // =====================================================================
        //  TICK LOOP
        // =====================================================================

        private void Tick(object state)
        {
            if (mDisposed) return;

            try
            {
                // 1) Drain command queue
                while (mPendingCommands.TryDequeue(out var cmd))
                    cmd();

                // 2) Process movement for all entities
                foreach (var kvp in mEntities)
                {
                    var ent = kvp.Value;
                    if (ent.IsRigid || ent.SpeedFraction <= 0) continue;

                    ProcessMovement(ent);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DestinyManager] Tick error in system {SolarSystemID}: {ex.Message}");
            }
        }

        private void ProcessMovement(BubbleEntity ent)
        {
            switch (ent.Mode)
            {
                case BallMode.Goto:
                    ProcessGoto(ent);
                    break;

                case BallMode.Follow:
                    ProcessFollow(ent);
                    break;

                case BallMode.Orbit:
                    ProcessOrbit(ent);
                    break;

                case BallMode.Warp:
                    ProcessWarp(ent);
                    break;

                case BallMode.Stop:
                    // Decelerate to zero
                    if (ent.Velocity.Length > 1.0)
                    {
                        ent.Velocity = ent.Velocity * 0.5;
                        ent.Position = ent.Position + ent.Velocity * TIC_DURATION;
                    }
                    else
                    {
                        ent.Velocity      = default;
                        ent.SpeedFraction = 0;
                    }
                    break;
            }

            // Check bubble transitions
            BubbleManager.UpdateEntityBubble(ent);
        }

        private void ProcessGoto(BubbleEntity ent)
        {
            Vector3 toTarget = ent.GotoTarget - ent.Position;
            double dist = toTarget.Length;

            if (dist < ARRIVE_DIST)
            {
                ent.Mode          = BallMode.Stop;
                ent.Velocity      = default;
                ent.SpeedFraction = 0;
                return;
            }

            Vector3 dir = toTarget.Normalize();
            double speed = ent.MaxVelocity * ent.SpeedFraction;
            ent.Velocity = dir * speed;
            ent.Position = ent.Position + ent.Velocity * TIC_DURATION;
        }

        private void ProcessFollow(BubbleEntity ent)
        {
            if (!mEntities.TryGetValue(ent.FollowTargetID, out var target))
            {
                ent.Mode = BallMode.Stop;
                return;
            }

            Vector3 toTarget = target.Position - ent.Position;
            double dist = toTarget.Length;

            if (dist <= ent.FollowRange + ARRIVE_DIST)
            {
                // Within range, slow down
                ent.Velocity = ent.Velocity * 0.5;
                ent.Position = ent.Position + ent.Velocity * TIC_DURATION;
                return;
            }

            Vector3 dir = toTarget.Normalize();
            double speed = ent.MaxVelocity * ent.SpeedFraction;
            ent.Velocity = dir * speed;
            ent.Position = ent.Position + ent.Velocity * TIC_DURATION;
        }

        private void ProcessOrbit(BubbleEntity ent)
        {
            if (!mEntities.TryGetValue(ent.FollowTargetID, out var target))
            {
                ent.Mode = BallMode.Stop;
                return;
            }

            Vector3 toTarget = target.Position - ent.Position;
            double dist = toTarget.Length;
            double desiredRange = ent.FollowRange > 0 ? ent.FollowRange : 5000;

            if (Math.Abs(dist - desiredRange) > ORBIT_TOLERANCE)
            {
                // Approach/retreat to orbit range
                Vector3 dir = toTarget.Normalize();
                double speed = ent.MaxVelocity * ent.SpeedFraction;
                double sign = dist > desiredRange ? 1.0 : -1.0;
                ent.Velocity = dir * (speed * sign);
            }
            else
            {
                // At orbit range - rotate around target
                Vector3 dir = toTarget.Normalize();
                // Perpendicular vector (rotate 90 degrees in XZ plane)
                Vector3 perp = new Vector3 { X = -dir.Z, Y = 0, Z = dir.X };
                double speed = ent.MaxVelocity * ent.SpeedFraction;
                ent.Velocity = perp * speed;
            }

            ent.Position = ent.Position + ent.Velocity * TIC_DURATION;
        }

        private void ProcessWarp(BubbleEntity ent)
        {
            Vector3 toTarget = ent.WarpTarget - ent.Position;
            double dist = toTarget.Length;

            // Arrive at warp destination
            if (dist < ARRIVE_DIST)
            {
                ent.Position      = ent.WarpTarget;
                ent.Mode          = BallMode.Stop;
                ent.Velocity      = default;
                ent.SpeedFraction = 0;

                Console.WriteLine($"[DestinyManager] Warp complete: entity {ent.ItemID} arrived at {ent.Position}");

                // Broadcast stop at destination
                var bubble = BubbleManager.GetBubbleForEntity(ent.ItemID);
                if (bubble != null)
                {
                    var events = DestinyEventBuilder.BuildSetSpeedFraction(ent.ItemID, 0);
                    mBroadcaster.BroadcastToSystem(SolarSystemID, events);
                }
                return;
            }

            // Move at warp speed toward target
            Vector3 dir = toTarget.Normalize();
            double moveDistance = Math.Min(WARP_SPEED * TIC_DURATION, dist);
            ent.Position = ent.Position + dir * moveDistance;
            ent.Velocity = dir * WARP_SPEED;
        }

        // =====================================================================
        //  CLEANUP
        // =====================================================================

        public bool HasPlayers
        {
            get
            {
                foreach (var kvp in mEntities)
                    if (kvp.Value.IsPlayer) return true;
                return false;
            }
        }

        public void Dispose()
        {
            if (mDisposed) return;
            mDisposed = true;
            mTickTimer?.Dispose();
            Console.WriteLine($"[DestinyManager] Disposed for system {SolarSystemID}");
        }
    }
}
