package xtdb.query;

import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.lang.Var;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import xtdb.IllegalArgumentException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WithDeserializer extends StdDeserializer<Query.With> {

    public WithDeserializer() {
        super(Query.With.class);
    }

    @Override
    public Query.With deserialize(JsonParser p, DeserializationContext ctxt) throws IllegalArgumentException, IOException {
        ObjectMapper mapper = (ObjectMapper) p.getCodec();
        JsonNode node = mapper.readTree(p);
        try {
            JsonNode with = node.get("with");

            if (with.isArray()) {
                return Query.with(SpecListDeserializer.<VarSpec>nodeToSpecs(mapper, with, VarSpec::of));
            } else {
                throw new IllegalArgumentException("With should be a list of bindings", PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), null);
            }
        } catch (IllegalArgumentException i) {
            throw i;
        } catch (Exception e) {
            throw IllegalArgumentException.create(Keyword.intern("xtql", "malformed-with"), PersistentHashMap.create(Keyword.intern("json"), node.toPrettyString()), e);
        }
    }
}