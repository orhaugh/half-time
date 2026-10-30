// The riders job: the riders' devices, read over MQTT, kept in the rider
// app's store in MongoDB, and a command back to a rider whose battery runs
// low.
//
// It is the night's one job built on clink's typed C++ API rather than in
// SQL, since neither MQTT nor MongoDB is reachable from SQL: a module,
// riders.so, built against clink's installed CMake package and submitted to
// the Coordinator with `clink run --job`. Its key and its two operators are
// registered here and run on the Workers; the MQTT source and the two sinks
// are clink's own connectors, named by their factories.
//
// Each device publishes its status to riders/<id>/status, retained. The job
// reads them all, keys them by rider, and hands each to two keyed operators:
// rider_state keeps the newest report per rider and writes it to the
// riders collection, replacing the rider's document; low_battery sends
// riders/commands a command to charge when a rider's battery falls below
// 15 per cent, and not again until it has been back at 30. Both judge a
// report by the device's clock, so a report the broker hands over again, or
// one older than what the rider has already sent, changes nothing.

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <stdexcept>
#include <string>

#include <clink/api/pipeline.hpp>
#include <clink/cluster/built_in_factories.hpp>
#include <clink/config/json.hpp>
#include <clink/core/codec.hpp>
#include <clink/job/register_job.hpp>
#include <clink/operators/operator_base.hpp>
#include <clink/state/keyed_state.hpp>

namespace halftime {

constexpr std::int64_t kLowBatteryPct = 15;  // tools/world.py LOW_BATTERY_PCT
constexpr std::int64_t kChargedPct = 30;     // tools/world.py CHARGED_PCT

// A device's status report, as tools/feed_devices.py publishes it.
struct Report {
    std::int64_t rider_id{-1};
    std::int64_t ts{0};  // the device's clock, milliseconds
    std::int64_t battery_pct{0};
    std::string cell;
    std::string state;  // riding | on_break
};

std::optional<Report> parse(const std::string& payload) {
    try {
        const auto js = clink::config::parse(payload);
        const auto& o = js.as_object();
        Report r;
        r.rider_id = o.at("rider_id").as_int();
        r.ts = o.at("ts").as_int();
        r.battery_pct = o.at("battery_pct").as_int();
        r.cell = o.at("cell").as_string();
        r.state = o.at("state").as_string();
        return r;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::string quoted(const std::string& s) {
    std::string out = "\"";
    for (const char c : s) {
        if (c == '"' || c == '\\') {
            out += '\\';
        }
        out += c;
    }
    return out + "\"";
}

// What both operators do with a punctuation: pass it on.
template <typename Op>
void forward(Op& op, const clink::StreamElement<std::string>& el, clink::Emitter<std::string>& out) {
    if (el.is_watermark()) {
        op.on_watermark(el.as_watermark(), out);
    } else {
        op.on_barrier(el.as_barrier(), out);
    }
}

// The newest report per rider, as the document the rider app's store keeps
// under the rider's id.
class RiderState final : public clink::Operator<std::string, std::string> {
public:
    void open() override {
        last_ts_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "riders.last_ts", clink::int64_codec(), clink::int64_codec()));
    }

    void process(const clink::StreamElement<std::string>& el,
                 clink::Emitter<std::string>& out) override {
        if (!el.is_data()) {
            forward(*this, el, out);
            return;
        }
        clink::Batch<std::string> docs;
        for (const auto& rec : el.as_data()) {
            const auto r = parse(rec.value());
            if (!r) {
                continue;
            }
            if (const auto last = last_ts_->get(r->rider_id); last && *last >= r->ts) {
                continue;
            }
            last_ts_->put(r->rider_id, r->ts);
            docs.emplace("{\"_id\":" + std::to_string(r->rider_id) +
                         ",\"rider_id\":" + std::to_string(r->rider_id) +
                         ",\"cell\":" + quoted(r->cell) +
                         ",\"battery_pct\":" + std::to_string(r->battery_pct) +
                         ",\"state\":" + quoted(r->state) + ",\"ts\":" + std::to_string(r->ts) +
                         "}");
        }
        if (!docs.empty()) {
            out.emit_data(std::move(docs));
        }
    }

    std::string name() const override { return "halftime.rider_state"; }

private:
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> last_ts_;
};

// A command to charge, once per time a rider's battery falls below 15 per
// cent; the rider is told again only after the battery has been back at 30.
class LowBattery final : public clink::Operator<std::string, std::string> {
public:
    void open() override {
        last_ts_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "riders.low_battery.last_ts", clink::int64_codec(), clink::int64_codec()));
        low_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "riders.low_battery.low", clink::int64_codec(), clink::int64_codec()));
    }

    void process(const clink::StreamElement<std::string>& el,
                 clink::Emitter<std::string>& out) override {
        if (!el.is_data()) {
            forward(*this, el, out);
            return;
        }
        clink::Batch<std::string> commands;
        for (const auto& rec : el.as_data()) {
            const auto r = parse(rec.value());
            if (!r) {
                continue;
            }
            if (const auto last = last_ts_->get(r->rider_id); last && *last >= r->ts) {
                continue;
            }
            last_ts_->put(r->rider_id, r->ts);
            const bool low = low_->get(r->rider_id).value_or(0) != 0;
            if (!low && r->battery_pct < kLowBatteryPct) {
                low_->put(r->rider_id, 1);
                commands.emplace("{\"rider_id\":" + std::to_string(r->rider_id) +
                                 ",\"ts\":" + std::to_string(r->ts) +
                                 ",\"battery_pct\":" + std::to_string(r->battery_pct) +
                                 ",\"command\":\"charge\"}");
            } else if (low && r->battery_pct >= kChargedPct) {
                low_->put(r->rider_id, 0);
            }
        }
        if (!commands.empty()) {
            out.emit_data(std::move(commands));
        }
    }

    std::string name() const override { return "halftime.low_battery"; }

private:
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> last_ts_;
    std::optional<clink::KeyedState<std::int64_t, std::int64_t>> low_;
};

std::string env_or(const char* name, const char* fallback) {
    const char* v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::string{v} : std::string{fallback};
}

void define_job(clink::api::Pipeline& pipeline) {
    // The channel types the key and the operators are declared on.
    clink::cluster::ensure_built_ins_registered();
    auto& reg = pipeline.registry();
    reg.register_key_extractor<std::string>("halftime.rider", [](const std::string& s) {
        const auto r = parse(s);
        return r ? r->rider_id : std::int64_t{-1};
    });
    reg.register_operator<std::string, std::string>(
        "halftime.rider_state",
        [](const clink::plugin::BuildContext&) { return std::make_shared<RiderState>(); });
    reg.register_operator<std::string, std::string>(
        "halftime.low_battery",
        [](const clink::plugin::BuildContext&) { return std::make_shared<LowBattery>(); });

    // Where the Workers reach the broker and the store, from the submitting
    // shell (run.sh sets them; a rig names its own hosts).
    const auto mqtt_host = env_or("HALFTIME_MQTT_HOST", "127.0.0.1");
    const auto mqtt_port = env_or("HALFTIME_MQTT_PORT", "1883");
    const auto mongo_uri = env_or("HALFTIME_MONGO_URI", "mongodb://127.0.0.1:27017/?directConnection=true");

    clink::api::SourceDescriptor devices;
    devices.op_type = "mqtt_source";
    devices.channel_type = "string";
    devices.params = {{"host", mqtt_host},
                      {"port", mqtt_port},
                      {"topic", "riders/+/status"},
                      {"qos", "1"},
                      {"client_id", "halftime-riders"}};
    auto keyed = pipeline.source<std::string>(devices).key_by("halftime.rider");

    auto latest = keyed.process<std::string>("halftime.rider_state", {}, "rider_state");
    latest.uid("halftime-riders-state");
    clink::api::SinkDescriptor store;
    store.op_type = "mongo_sink";
    store.channel_type = "string";
    store.params = {{"uri", mongo_uri},
                    {"database", "riderapp"},
                    {"collection", "riders"},
                    {"on_duplicate", "replace"},
                    {"key_field", "_id"},
                    {"linger_ms", "500"}};
    latest.sink(store, "riders_store");

    auto commands = keyed.process<std::string>("halftime.low_battery", {}, "low_battery");
    commands.uid("halftime-riders-low-battery");
    clink::api::SinkDescriptor command_bus;
    command_bus.op_type = "mqtt_sink";
    command_bus.channel_type = "string";
    command_bus.params = {{"host", mqtt_host},
                          {"port", mqtt_port},
                          {"topic", "riders/commands"},
                          {"qos", "1"},
                          {"client_id", "halftime-riders-commands"},
                          {"linger_ms", "500"}};
    commands.sink(command_bus, "riders_commands");
}

}  // namespace halftime

CLINK_REGISTER_JOB("halftime-riders",
                   "1.0",
                   "rider devices over MQTT -> the rider app's store in MongoDB, and low-battery "
                   "commands back over MQTT",
                   halftime::define_job);
