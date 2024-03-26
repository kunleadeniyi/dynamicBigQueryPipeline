//package events;
//
//import java.util.*;
//import java.util.concurrent.ThreadLocalRandom;
//
//public class GenerateEvents {
//    private final static List<String> SERVERS = getUUIDs("servers", 3);
//    private final static List<String> MATCHES = getUUIDs("matches", 50);
//
//    private final List<Integer> levels = Arrays.asList(1,2,3,4,5);
//    private final List<String> countries =  Arrays.asList("UK", "UNITED STATES", "JAPAN", "SINGAPORE", "AUSTRALIA", "BRAZIL", "SOUTH KOREA", "GERMANY", "CANADA", "FRANCE");
//    private final List<String> items = getUUIDs("items", 10);
//    private final List<String> currencies = Arrays.asList("USD", "EUR", "YEN", "RMB");
//    private final List<String> platforms = Arrays.asList("nintendo_switch", "ps4", "xbox_360", "iOS", "android", "pc", "fb_messenger");
//    private final List<String> tutorialScreens = Arrays.asList("1_INTRO", "2_MOVEMENT", "3_WEAPONS", "4_FINISH");
//    private final List<String> matchTypes = Arrays.asList("1v1", "TEAM_DM_5v5", "CTF");
//    private final List<String> matchingFailedMsg = Arrays.asList( "timeout", "user_quit", "too_few_users" );
//    private final List<String> maps = Arrays.asList("WAREHOUSE", "CASTLE", "AIRPORT");
//    private final List<String> gameResults = Arrays.asList("WIN", "LOSE", "KICKED", "DISCONNECTED", "QUIT");
//    private final List<String> spells = Arrays.asList("WATER", "EARTH", "FIRE", "AIR");
//    private final List<String> ranks = Arrays.asList("1_BRONZE", "2_SILVER", "3_GOLD", "4_PLATINUM", "5_DIAMOND", "6_MASTER");
//    private final List<String> itemRarities = Arrays.asList("COMMON", "UNCOMMON", "RARE", "LEGENDARY");
//    private final List<String> reportReasons = Arrays.asList("GRIEFING", "CHEATING", "AFK", "RACISM/HARASSMENT");
//    private final static String DEFAULT_EVENT_VERSION = "v1";
//    private static String PREVIOUS_APP_VERSION;
//    private static String CURRENT_APP_VERSION = updateAppVersion();
//    private final Random rand = new Random();
//
//    public static String updateAppVersion() {
//        String newVersion;
//        if (PREVIOUS_APP_VERSION != null) {
//            String majorRelease = PREVIOUS_APP_VERSION.substring(0, PREVIOUS_APP_VERSION.length() - 1);
//            String minorVersionCharacter = PREVIOUS_APP_VERSION.substring(PREVIOUS_APP_VERSION.length() - 1);
//            int newMinorVersionCharacter = Integer.parseInt(minorVersionCharacter) + 1;
//
//            newVersion = majorRelease + newMinorVersionCharacter;
//            PREVIOUS_APP_VERSION = CURRENT_APP_VERSION;
//        } else {
//            newVersion = "1.0.0";
//        }
//        return newVersion;
//    }
//
//    public static List<String> getUUIDs (String dataType, int count) {
//        List<String> uuids = new ArrayList<String>();
//
//        for (int i = 0; i < count; i++) {
//            uuids.add(UUID.randomUUID().toString());
//        }
//
//        return uuids;
//    }
//
//    public static String getEventType() {
//        Map<Integer, String> eventTypes = new HashMap<>();
//
//        eventTypes.put(1, "user_registration");
//        eventTypes.put(2, "user_knockout");
//        eventTypes.put(3, "item_viewed");
//        eventTypes.put(4, "iap_transaction");
//        eventTypes.put(5, "login");
//        eventTypes.put(6, "logout");
//        eventTypes.put(7, "tutorial_progression");
//        eventTypes.put(8, "user_rank_up");
//        eventTypes.put(9, "matchmaking_start");
//        eventTypes.put(10, "matchmaking_complete");
//        eventTypes.put(11, "matchmaking_failed");
//        eventTypes.put(12, "match_start");
//        eventTypes.put(13, "match_end");
//        eventTypes.put(14, "level_started");
//        eventTypes.put(15, "level_completed");
//        eventTypes.put(16, "level_failed");
//        eventTypes.put(17, "lootbox_opened");
//        eventTypes.put(18, "user_report");
//        eventTypes.put(19, "user_sentiment");
//
//        int randomNum = ThreadLocalRandom.current().nextInt(1, 20);
//        // System.out.println("Random number is: " + randomNum);
//        return eventTypes.get(randomNum);
//    }
//
//    public Map<String, Object> login () {
//        Map<String, Object> eventData = new HashMap<>();
//        eventData.put("platform", platforms.get(rand.nextInt(0, platforms.size())));
//        eventData.put("last_login_time", System.currentTimeMillis() - rand.nextInt(4000000 - 40000) + 40000);
//        return eventData;
//    }
//
//    public Map<String, Object> logout () {
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("last_screen_seen", "Logout Succeeded");
//        eventData.put("logout_time", System.currentTimeMillis());
//        return eventData;
//    }
//
//    public Map<String, Object> client_latency () {
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("latency", rand.nextInt( 20, 200));
//        eventData.put("connected_server_id", SERVERS.get(rand.nextInt(0, SERVERS.size())));
//        eventData.put("region", countries.get(rand.nextInt(0, countries.size())));
//
//        return eventData;
//    }
//
//    public Map<String, Object> user_registration (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("country_id", countries.get(rand.nextInt(0, countries.size())));
//        eventData.put("platform", platforms.get(rand.nextInt(0, platforms.size())));
//
//        return eventData;
//    }
//
//    public Map<String, Object> user_knockout (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("match_id", MATCHES.get(rand.nextInt(0, MATCHES.size())));
//        eventData.put("map_id", maps.get(rand.nextInt(0, maps.size())));
//        eventData.put("spell_id", spells.get(rand.nextInt(0, spells.size())));
//        eventData.put("exp_gained", rand.nextInt(0, 2));
//
//        return eventData;
//    }
//
//    public Map<String, Object> item_viewed (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("item_id", items.get(rand.nextInt(0, items.size())));
//        eventData.put("item_version", rand.nextInt(1, 3));
//
//        return eventData;
//    }
//
//    public Map<String, Object> iap_transaction (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("item_id", items.get(rand.nextInt(0, items.size())));
//        eventData.put("item_version", rand.nextInt(1, 3));
//        eventData.put("item_amount", rand.nextInt(1, 5));
//        eventData.put("currency_type", currencies.get(rand.nextInt(0, currencies.size())));
//        eventData.put("country_id", countries.get(rand.nextInt(0, countries.size())));
//        eventData.put("currency_amount", rand.nextInt(1, 10));
//        eventData.put("transaction_id", UUID.randomUUID().toString());
//
//        return eventData;
//    }
//
//    public Map<String, Object> tutorial_progression (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("tutorial_screen_id", tutorialScreens.get(rand.nextInt(0, tutorialScreens.size())));
//        eventData.put("tutorial_screen_version", rand.nextInt(1, 3));
//
//        return eventData;
//    }
//
//    public Map<String, Object> user_rank_up (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("user_rank_reached", ranks.get(rand.nextInt(0, ranks.size())));
//
//        return eventData;
//    }
//
//    public Map<String, Object> matchmaking_start (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("match_id", MATCHES.get(rand.nextInt(0, MATCHES.size())));
//        eventData.put("match_type", matchTypes.get(rand.nextInt(0, matchTypes.size())));
//
//        return eventData;
//    }
//
//    public Map<String, Object> matchmaking_complete (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("match_id", MATCHES.get(rand.nextInt(0, MATCHES.size())));
//        eventData.put("match_type", matchTypes.get(rand.nextInt(0, matchTypes.size())));
//        eventData.put("matched_slots", rand.nextInt(6, 10));
//
//        return eventData;
//    }
//
//    public Map<String, Object> matchmaking_failed (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("match_id", MATCHES.get(rand.nextInt(0, MATCHES.size())));
//        eventData.put("match_type", matchTypes.get(rand.nextInt(0, matchTypes.size())));
//        eventData.put("matched_slots", rand.nextInt(6, 10));
//        eventData.put("matching_failed_msg", matchingFailedMsg.get(rand.nextInt(0, matchingFailedMsg.size())));
//
//        return eventData;
//    }
//
//    public Map<String, Object> match_start (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("match_id", MATCHES.get(rand.nextInt(0, MATCHES.size())));
//        eventData.put("map_id", matchTypes.get(rand.nextInt(0, matchTypes.size())));
//
//        return eventData;
//    }
//
//    public Map<String, Object> match_end (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("match_id", MATCHES.get(rand.nextInt(0, MATCHES.size())));
//        eventData.put("map_id", matchTypes.get(rand.nextInt(0, matchTypes.size())));
//        eventData.put("match_result_type", gameResults.get(rand.nextInt(0, gameResults.size())));
//        eventData.put("exp_gained", rand.nextInt(100, 200));
//        eventData.put("most_used_spell", spells.get(rand.nextInt(0, spells.size())));
//
//        return eventData;
//    }
//
//
//    public Map<String, Object> level_started (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("level_id", levels.get(rand.nextInt(0, levels.size())));
//        eventData.put("level_version", rand.nextInt(1, 3));
//
//        return eventData;
//    }
//
//    public Map<String, Object> level_completed (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("level_id", levels.get(rand.nextInt(0, levels.size())));
//        eventData.put("level_version", rand.nextInt(1, 3));
//
//        return eventData;
//    }
//
//    public Map<String, Object> level_failed (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("level_id", levels.get(rand.nextInt(0, levels.size())));
//        eventData.put("level_version", rand.nextInt(1, 3));
//
//        return eventData;
//    }
//
//    public Map<String, Object> lootbox_opened (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("lootbox_id", UUID.randomUUID().toString());
//        eventData.put("lootbox_cost", rand.nextInt(2, 6));
//        eventData.put("item_rarity", itemRarities.get(rand.nextInt(0, itemRarities.size())));
//        eventData.put("item_id", items.get(rand.nextInt(0, items.size())));
//        eventData.put("item_version", rand.nextInt(1, 3));
//        eventData.put("item_amount", rand.nextInt(1, 5));
//
//        return eventData;
//    }
//
//    public Map<String, Object> user_report (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("report_id", UUID.randomUUID().toString());
//        eventData.put("report_reason", reportReasons.get(rand.nextInt(0, reportReasons.size())));
//
//        return eventData;
//    }
//
//    public Map<String, Object> user_sentiment (){
//        Map<String, Object> eventData = new HashMap<>();
//
//        eventData.put("user_rating", rand.nextInt(0, 6));
//
//        return eventData;
//    }
//
//
//
//    public Map<String, Object> /* Map<String, Map<String, Object>> */ getEvent(String eventType) {
//
//        Map<String, Map<String, Object>> switcher = new HashMap<>();
//
//        switcher.put("login", login());
//        switcher.put("logout", logout());
//        switcher.put("client_latency", client_latency());
//        switcher.put("user_registration", user_registration());
//        switcher.put("user_knockout", user_knockout());
//        switcher.put("item_viewed", item_viewed());
//        switcher.put("iap_transaction", iap_transaction());
//        switcher.put("tutorial_progression", tutorial_progression());
//
//        switcher.put("user_rank_up", user_rank_up());
//        switcher.put("matchmaking_start", matchmaking_start());
//        switcher.put("matchmaking_complete", matchmaking_complete());
//        switcher.put("matchmaking_failed", matchmaking_failed());
//        switcher.put("match_start", match_start());
//        switcher.put("match_end", match_end());
//
//        switcher.put("level_started", level_started());
//        switcher.put("level_completed", level_completed());
//        switcher.put("level_failed", level_failed());
//
//        switcher.put("lootbox_opened", lootbox_opened());
//        switcher.put("user_report", user_report());
//        switcher.put("user_sentiment", user_report());
//
//        return switcher.get(eventType);
//    }
//
//    public Map<String, Object> generateEvent () {
//        String eventType = getEventType();
//
//        Map<String, Object>/*<String, Map<String, Object>>*/ event = new HashMap<>();
//        Map<String, Object> eventData = getEvent(eventType);
//        // returning an immutable map object but I want to add more stuff to it, need it to be mutable
//
//        // System.out.println(eventData.get("eventData"));
//        // System.out.println(eventData.get("eventData").getClass());
//
//        eventData.put("event_version", DEFAULT_EVENT_VERSION);
//        eventData.put("event_id", UUID.randomUUID().toString());
//        eventData.put("event_timestamp", System.currentTimeMillis());
//        eventData.put("app_version", CURRENT_APP_VERSION);
//
//        // add some fields to eventData
//        event.put("eventName", eventType);
//        event.put("eventData", eventData);
//
//        // System.out.println(event);
//        // System.out.println(event.get("eventData"));
//        return event;
//    }
//
//
//    public void main(String[] args) {
//        // List<String> items = getUUIDs("items", 10);
//        // System.out.println(items);
//
//        // System.out.println(getEventType());
//        // System.out.println(getEvent(getEventType()));
//
////        String eventType = getEventType();
////        Map<String, Object> event = getEvent(eventType);
////        System.out.println(getEvent("login"));
////
////        System.out.println(eventType);
////        System.out.println(event);
////        System.out.println(event.get("eventData"));
//
//        // Random rand = new Random();
//        // System.out.println(rand.nextInt( 0, SERVERS.size()));
//
////        while (true) {
////            String eventType = getEventType();
////            Map<String, Object> event = getEvent(eventType);
////            // System.out.println(getEvent("login"));
////
////            System.out.println(eventType);
////            System.out.println(event);
////            System.out.println(event.get("eventData"));
////        }
//
//        System.out.println(generateEvent());
////        System.out.println(CURRENT_APP_VERSION);
//    }
//
//}
