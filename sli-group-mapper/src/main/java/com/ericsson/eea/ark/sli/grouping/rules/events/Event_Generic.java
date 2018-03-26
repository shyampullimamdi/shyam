package com.ericsson.eea.ark.sli.grouping.rules.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bigdata.common.util.MatchableMap;
import com.ericsson.bigdata.esr.parserlogic.CastingException;

/**
 * @author ejoepao This is class to support creating generic object as
 *         {@code MatchableMap<String, Object>} which can be used to support complex
 *         structure. The complex structure can defined via a csv file . Generic
 *         objects can be defined in the input.generic.facts_directory in the
 *         config.txt.
 */
public class Event_Generic {
    private static final Logger log = LoggerFactory
            .getLogger(Event_Generic.class.getName());

    private Long timestamp = null;
    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    private String name=null;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private  MatchableMap<String, Object> map = null;

    public  MatchableMap<String, Object> getMap() {
        return map;
    }

    public Event_Generic(Long timestamp, String name,
            MatchableMap<String, Object> map) {
        super();
        this.timestamp = timestamp;
        this.name = name;
        this.map = map;
    }

    @Override
    public String toString() {
        return "EventType_Generic [timestamp=" + timestamp + ", name=" + name
                + ", map=" + map + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((map == null) ? 0 : map.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result
                + ((timestamp == null) ? 0 : timestamp.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Event_Generic other = (Event_Generic) obj;
        if (map == null) {
            if (other.map != null)
                return false;
        } else if (!map.equals(other.map))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (timestamp == null) {
            if (other.timestamp != null)
                return false;
        } else if (!timestamp.equals(other.timestamp))
            return false;
        return true;
    }

    public void setMap(MatchableMap<String, Object> map) {
        this.map = map;
    }

    public Event_Generic() {
        super();
    }

    public Object get(String key) {
        if (map != null && !map.isEmpty()) {
            if (map.get(key) != null && map.get(key) instanceof String
                    && ((String) map.get(key)).isEmpty()) {
                //return a null instead because rules writer are trained to check for null
                return null;
            }
            return map.get(key);
        }
        return null;

    }

    public  <T> T get(String key, Class<T> type) {
        Object o = this.get(key);
        if (o == null)
            return null;

        try {
            return Event_Generic.getValue(o, type);
        } catch (CastingException e) {
            //Put it just as debug as it may not be considered an error.
            log.warn("casting exception trying to get Type value for key=" + key +
                    " type=" + type + " object=" + o) ;
        }
        return null;

    }

    /**
     * Generic casting method for all types.
     * @param o - object to be casted
     * @param type - target class
     * @return kpi value or record
     * @throws CastingException
     */
    @SuppressWarnings("unchecked")
    public static <T> T getValue(Object o, Class<T> type) throws CastingException {
        // o is null
        if (o == null) {
            return null;
        }

        // T is String
        else if (type == String.class) {
            if (o instanceof String) {
                return (((String) o).isEmpty() ? null : (T) o);
            }
            else if (o instanceof Integer) {
                return (((Integer) o).toString().isEmpty() ? null : (T)((Integer) o).toString());
            }
            else if (o instanceof Long) {
                return (((Long) o).toString().isEmpty() ? null : (T)((Long) o).toString());
            }
            else if (o instanceof Double) {
                return (((Double) o).toString().isEmpty() ? null : (T)((Double) o).toString());
            }
            else
                throw new CastingException(o.toString() + " cannot be cast to String.");
        }
        // T is Long
        else if (type == Long.class) {
            if (o instanceof Long) {
                return (T) o;
            }
            else if (o instanceof Integer) {
                return (T) (new Long(((Integer) o).longValue()));
            }
            else if (o instanceof java.lang.Double) {
                double d = (Double) o;
                if (d == Math.floor(d)) {
                    return (T) (new Long((long) d));
                }
            }
            else if (o instanceof String) {
                if (((String) o).isEmpty())
                    return null;
                try {
                    return (T) (new Long((String)o));
                } catch(Exception e) {}
            }
            throw new CastingException(o.toString() + " cannot be cast to Long.");
        }
        // T is Double
        else if (type == Double.class) {
            if (o instanceof Double) {
                return (T) o;
            }
            else if (o instanceof Integer) {
                return (T) (new Double(((Integer) o).doubleValue()));
            }
            else if (o instanceof Long) {
                return (T) (new Double(((Long) o).longValue()));
            }
            else if (o instanceof String) {
                if (((String) o).isEmpty())
                    return null;

                try {
                    return (T) (new Double((String)o));
                } catch(Exception e) {}
            }
            else
                throw new CastingException(o.toString() + " cannot be cast to Double.");
        }
        // T is Integer
        if (type == Integer.class) {
            if (o instanceof Integer) {
                return (T) o;
            }
            else if (o instanceof Long) {
                long l = (Long) o;
                if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
                    throw new CastingException(l + " cannot be cast to int without changing its value.");
                }
                return (T) (new Integer((int) l));
            }
            else if (o instanceof String) {	// This is needed to handle integers used as keys in a JSON Map
                if (((String) o).isEmpty())
                    return null;
                try {
                    return (T) (new Integer((String) o));
                } catch(Exception e) {}
            }
            else if (o instanceof Double) {
                double d = (Double) o;
                if (d == Math.floor(d)) {
                    return (T) getValue(new Long((long) d), Integer.class);
                }
            }
            throw new CastingException(o.toString() + " cannot be cast to Integer.");
        }
        throw new CastingException("Cannot cast to type " + type.getName());
    }


}
