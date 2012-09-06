using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Trinity.Configuration;
using Trinity.Data;
using Trinity.DataModel;
using Trinity.Core;
using Trinity.Storage;

namespace Friends
{
    class Program
    {
        static void Main(string[] args)
        {
            TrinityConfig.CurrentRunningMode = RunningMode.Embedded;

            CastCell Jennifer = new CastCell("Jennifer Aniston", 43);
            CastCell Courteney = new CastCell("Courteney Cox", 43);
            CastCell Lisa = new CastCell("Lisa Kudrow", 43);
            CastCell Matt = new CastCell("Matt Le Blanc", 43);
            CastCell Matthew = new CastCell("Matthew Perry", 43);
            CastCell David = new CastCell("David Schwimmer", 43);

            CharacterCell Rachel = new CharacterCell("Rachel Green", 0);
            CharacterCell Monica = new CharacterCell("Monica Geller", 0);
            CharacterCell Phoebe = new CharacterCell("Phoebe Buffay", 0);
            CharacterCell Joey = new CharacterCell("Joey Tribbiani", 1);
            CharacterCell Chandler = new CharacterCell("Chandler Bing", 1);
            CharacterCell Ross = new CharacterCell("Ross Geller", 1);

            CastCell[] Casts = new CastCell[] { Jennifer, Courteney, Lisa, Matt, Matthew, David };
            CharacterCell[] Characters = new CharacterCell[] { Rachel, Monica, Phoebe, Joey, Chandler, Ross };

            int cell_id = 0;

            for (int i = 0; i < Casts.Length; i++)
            {
                Global.LocalStorage.SaveCastCell(cell_id++, Casts[i]);
            }

            for (int i = 0; i < Characters.Length; i++)
            {
                Global.LocalStorage.SaveCharacterCell(cell_id++, Characters[i]);
            }

            //FriendsRelation friends_relation = new FriendsRelation(
            //    new List<long>(

            //        );
        }
    }
}
