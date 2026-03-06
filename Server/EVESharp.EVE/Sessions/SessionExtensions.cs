using EVESharp.EVE.Exceptions;

namespace EVESharp.EVE.Sessions;

public static class SessionExtensions
{
    /// <summary>
    /// Checks session data to ensure a character is selected and returns it's characterID
    /// </summary>
    /// <returns>CharacterID for the client</returns>
    public static int EnsureCharacterIsSelected (this Session session)
    {
        if (session.ContainsKey (Session.CHAR_ID) == false)
            throw new CustomError ("NoCharacterSelected");

        return session.CharacterID;
    }

    /// <summary>
    /// Checks session data to ensure the character is in a station
    /// </summary>
    /// <returns>The StationID where the character is at</returns>
    /// <exception cref="CanOnlyDoInStations"></exception>
    public static int EnsureCharacterIsInStation (this Session session)
    {
        if (session.ContainsKey (Session.STATION_ID) == false)
            throw new CanOnlyDoInStations ();

        return session.StationID;
    }

    /// <summary>
    /// Checks session data to ensure the character is in space (has a solar system, not in a station)
    /// </summary>
    /// <returns>The SolarSystemID where the character is at</returns>
    /// <exception cref="CustomError">Thrown when not in space</exception>
    public static int EnsureCharacterIsInSpace (this Session session)
    {
        int? solarSystemID = session.SolarSystemID;

        if (solarSystemID == null)
            throw new CustomError ("You must be in space to do this");

        return solarSystemID.Value;
    }
}