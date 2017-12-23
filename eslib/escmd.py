#!/usr/bin/env python

import os
import sys
import optparse
import eslib
import collections
import codecs

from eslib.context import Context, ConfigurationError

def safe_print(string):
    """Ensure that string will print without error if it contains unprintable characters"""
    encoded = codecs.encode(string, sys.stdout.encoding, 'replace')
    decoded = codecs.decode(encoded, sys.stdout.encoding, 'replace')
    print(decoded)

def print_run_phrase(dispatcher, verb, object_options={}, object_args=[]):
    (cmd, executed) = dispatcher.run_phrase(verb, object_options, object_args)
    if cmd is None:
        print("invalid phrase '%s %s'" % (dispatcher.object_name, verb))
        return 255
    if isinstance(executed, collections.Iterable) and not isinstance(executed, (str, bytes, dict)):
        # If execute return a generator, iterate other it
        for s in executed:
            if s != None:
                string = cmd.to_str(s)
                if string:
                    safe_print(string)
                    sys.stdout.flush()
        return cmd.status()
    elif executed is not None and executed is not False:
        # Else if it return something, just print it
        string = cmd.to_str(executed)
        if string:
            safe_print(string)
        return cmd.status()
    elif executed is not None:
        # It return false, something went wrong
        print("'%s %s' failed" % (dispatcher.object_name, verb))
        return cmd.status()
    else:
        # It returned nothing, it should be OK.
        return 0


# needed because dict.update is using shortcuts and don't works on subclass of dict
class UpdateDict(dict):
    def update(self, other=None, **kwargs):
        if other is not None:
            for k in other:
                self[k] = other[k]
        super(UpdateDict, self).update(kwargs)


def main():

    default_config = None
    if 'ESCONFIG' in os.environ:
        default_config = os.environ['ESCONFIG']

    usage_common = "usage: %prog [options] object [object_args] verb [verbs_args]"
    #The first level parser
    parser = optparse.OptionParser(usage="%s\nobjects are:\n    %s" % (usage_common, "\n    ".join(list(eslib.dispatchers.keys()))))
    parser.disable_interspersed_args()
    parser.add_option("-c", "--config", dest="config_file", help="an alternative config file", default=default_config)
    parser.add_option("-d", "--debug", dest="debug", help="The debug level", action="store_true")
    parser.add_option("--passwordfile", dest="passwordfile", help="Read the password from that file")
    parser.add_option("-u", "--user", "--username", dest="username", help="User to authenticate")
    parser.add_option("-k", "--kerberos", dest="kerberos", help="Uses kerberos authentication", action='store_true')

    (options, args) = parser.parse_args()

    #Extract the context options from the first level arguments
    context_args = {k: v for k, v in list(vars(options).items()) if v is not None}

    if len(args) > 0:
        #A object is found try to resolve the verb

        object_name = args.pop(0)
        if object_name in eslib.dispatchers:
            dispatcher = eslib.dispatchers[object_name]()
        else:
            print(('unknown object: %s' % object_name))
            return 253
        #The object parser
        parser_object = optparse.OptionParser()
        parser_object.disable_interspersed_args()
        dispatcher.fill_parser(parser_object)
        parser_object.set_usage("%s\nverbs are:\n    %s" % (usage_common, "\n    ".join(list(dispatcher.verbs.keys()))))

        (object_options, object_args) = parser_object.parse_args(args)

        if len(object_args) > 0:
            verb = object_args.pop(0)
            try:
                context = Context(**context_args)
            except ConfigurationError as e:
                print(e.error_message)
                return 253
            try:
                context.connect()
                dispatcher.api = context
                object_options = vars(object_options)
                for k,v in list(object_options.items()):
                    if v is None:
                        del object_options[k]

                # run the found command and print the result
                status = print_run_phrase(dispatcher, verb, object_options, object_args)
                sys.exit(status)
            except (eslib.ESLibError) as e:
                print(e)
                print("The action \"%s %s\" failed with \n    %s" % (dispatcher.object_name, verb, e.error_message))
                return 251
            finally:
                if context is not None:
                    context.disconnect()
        else:
            print('verb missing')
    else:
        print('object missing')
    return 253

def main_wrap():
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    main_wrap()