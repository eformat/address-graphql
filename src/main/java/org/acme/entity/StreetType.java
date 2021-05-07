package org.acme.entity;

import java.util.HashSet;

/*
quarkus_test=# select distinct street_type_code from address;
(195 rows)
 */
public class StreetType {
    private static HashSet<String> streetType;
    private static StreetType self = null;

    public static StreetType instance() {
        if (self == null)
            self = new StreetType();
        return self;
    }

    private StreetType() {
        streetType = new HashSet<String>();
        streetType.add("bank");
        streetType.add("divide");
        streetType.add("throughway");
        streetType.add("lane");
        streetType.add("strait");
        streetType.add("pocket");
        streetType.add("wynd");
        streetType.add("pathway");
        streetType.add("bay");
        streetType.add("waterway");
        streetType.add("cruiseway");
        streetType.add("round");
        streetType.add("rising");
        streetType.add("way");
        streetType.add("frontage");
        streetType.add("woods");
        streetType.add("connection");
        streetType.add("centreway");
        streetType.add("course");
        streetType.add("commons");
        streetType.add("corner");
        streetType.add("mall");
        streetType.add("vista");
        streetType.add("gully");
        streetType.add("brae");
        streetType.add("parade");
        streetType.add("brow");
        streetType.add("haven");
        streetType.add("square");
        streetType.add("motorway");
        streetType.add("slope");
        streetType.add("bowl");
        streetType.add("causeway");
        streetType.add("dale");
        streetType.add("precinct");
        streetType.add("centre");
        streetType.add("retreat");
        streetType.add("ride");
        streetType.add("mead");
        streetType.add("glade");
        streetType.add("tarn");
        streetType.add("firetrack");
        streetType.add("top");
        streetType.add("avenue");
        streetType.add("spur");
        streetType.add("manor");
        streetType.add("return");
        streetType.add("meander");
        streetType.add("concord");
        streetType.add("distributor");
        streetType.add("tor");
        streetType.add("passage");
        streetType.add("dock");
        streetType.add("hill");
        streetType.add("place");
        streetType.add("path");
        streetType.add("domain");
        streetType.add("quays");
        streetType.add("keys");
        streetType.add("track");
        streetType.add("common");
        streetType.add("nook");
        streetType.add("follow");
        streetType.add("twist");
        streetType.add("crossing");
        streetType.add("harbour");
        streetType.add("corso");
        streetType.add("landing");
        streetType.add("park");
        streetType.add("dash");
        streetType.add("valley");
        streetType.add("line");
        streetType.add("chase");
        streetType.add("ridge");
        streetType.add("access");
        streetType.add("quay");
        streetType.add("quadrant");
        streetType.add("boardwalk");
        streetType.add("hub");
        streetType.add("ramp");
        streetType.add("alley");
        streetType.add("loop");
        streetType.add("junction");
        streetType.add("walkway");
        streetType.add("entrance");
        streetType.add("copse");
        streetType.add("serviceway");
        streetType.add("ramble");
        streetType.add("walk");
        streetType.add("estate");
        streetType.add("end");
        streetType.add("cul-de-sac");
        streetType.add("key");
        streetType.add("drive");
        streetType.add("brace");
        streetType.add("grange");
        streetType.add("subway");
        streetType.add("hollow");
        streetType.add("gap");
        streetType.add("expressway");
        streetType.add("arterial");
        streetType.add("highway");
        streetType.add("dene");
        streetType.add("roads");
        streetType.add("gate");
        streetType.add("row");
        streetType.add("amble");
        streetType.add("mews");
        streetType.add("heath");
        streetType.add("dip");
        streetType.add("ford");
        streetType.add("fireline");
        streetType.add("courtyard");
        streetType.add("laneway");
        streetType.add("wharf");
        streetType.add("terrace");
        streetType.add("circle");
        streetType.add("trunkway");
        streetType.add("firetrail");
        streetType.add("gardens");
        streetType.add("road");
        streetType.add("crest");
        streetType.add("edge");
        streetType.add("broadway");
        streetType.add("point");
        streetType.add("outlet");
        streetType.add("route");
        streetType.add("green");
        streetType.add("island");
        streetType.add("fairway");
        streetType.add("glen");
        streetType.add("boulevard");
        streetType.add("driveway");
        streetType.add("esplanade");
        streetType.add("reach");
        streetType.add("cross");
        streetType.add("freeway");
        streetType.add("deviation");
        streetType.add("foreshore");
        streetType.add("trail");
        streetType.add("crief");
        streetType.add("rise");
        streetType.add("rest");
        streetType.add("heights");
        streetType.add("down");
        streetType.add("crescent");
        streetType.add("view");
        streetType.add("turn");
        streetType.add("lookout");
        streetType.add("court");
        streetType.add("pursuit");
        streetType.add("concourse");
        streetType.add("fork");
        streetType.add("flat");
        streetType.add("vale");
        streetType.add("circuit");
        streetType.add("bend");
        streetType.add("garden");
        streetType.add("cutting");
        streetType.add("north");
        streetType.add("elbow");
        streetType.add("gateway");
        streetType.add("approach");
        streetType.add("break");
        streetType.add("close");
        streetType.add("outlook");
        streetType.add("river");
        streetType.add("extension");
        streetType.add("banan");
        streetType.add("lynne");
        streetType.add("bypass");
        streetType.add("busway");
        streetType.add("beach");
        streetType.add("run");
        streetType.add("cove");
        streetType.add("port");
        streetType.add("strip");
        streetType.add("pass");
        streetType.add("grove");
        streetType.add("parkway");
        streetType.add("boulevarde");
        streetType.add("dell");
        streetType.add("reserve");
        streetType.add("circus");
        streetType.add("link");
        streetType.add("street");
        streetType.add("easement");
        streetType.add("arcade");
        streetType.add("views");
        streetType.add("promenade");
        streetType.add("waters");
        streetType.add("villa");
        streetType.add("cluster");
        streetType.add("plaza");
    }

    private static boolean contains(String search) {
        return streetType.contains(search);
    }

    public static String matches(String search) {
        for(String stype : streetType){
            if (search.contains(stype)) {
                return stype;
            }
        }
        return new String();
    }
}
