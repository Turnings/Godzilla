#
# Thin command line argument parser
#
# Jan 2017 Joshua Chen <iesugrace@gmail.com>
#
# This software may be used and distributed according to
# the terms of the GNU General Public License.
#

import re


class ArgParser:

    def parse_args(self, args, request, preserve=False):

        """'args' is the argument list to be parsed,
        'request' defines how the arguments should be parsed.

        Example of the request:

        request = {
            'pattern': {'flag': ['--pattern', '-p'],
                        'arg': 1, 'multi': True},
            'all_match': {'flag': '--all-match'},
            'btime': {'flag': ['-bt', '--birth-time'],
                      'arg': 1, 'multi': True},
            'mtime': {'flag': ['-mt', '--modify-time'],
                      'arg': 1, 'multi': True},
            'sort': {'flag': ['--sort', '-s'], 'arg': 1, 'multi': True},
            'limit': {'flag': ['--limit', '-n'], 'arg': 1},
            'last_n': {'flag': '-[0-9]+$', 'arg': 2},
        }

        The key of the request is the name of the option, the value
        of each key is the definition of each option, it also is a dict.

        flag, signifies the start of an option, can be one of these:
            1. long form with argument: --option-name=value,
                                        --option-name value
            2. long form without argument: --option-name
            3. short form with argument: -o v, -ov, -opt v, -optv
            4. short form without argument: -o, -opt
            5. key and value: key=value

        Detection will be performed on the argument list, to avoid
        conflict between the two short forms. According to the rule,
        -o can not be used with -opt, because there is no way to
        recognize the -opt as option -opt, or option -o with an
        argument 'pt'.

        arg, defines if the option requires an argument, defaults to 0.
        possible values of the argument:

        0: no argument
        1: argument required
        2: option flag is a re, the flag itself is an argument
        3: argument is optional, equal sign required before the argument

        multi, whether the option can be specified multiple times to
        collect multiple values, defaults to False.

        -- (double dashes) can be added to the argument list to separate
        the options (with their arguments) from the non-option arguments.

        By default, unrecognized options trigger exception, if preserve
        is True, all unrecognized options will be preserved and
        returned. An option is determied by pattern '^(-|\w+=)'.

        Any parsed option and optionally its argument are consumed, the
        remaining arguments are the non-option arguments, if there is
        any such argument starts with a dash, it must be placed after
        the double dashes, or it will be deemed to be an unrecognized
        option, when 'preserve' is True, unrecognized options can be
        returned for further parsing.
        """

        # work on an copy,
        # preserve the order of each option/argument
        args = args[:]
        args = list(enumerate(args))

        self.detect_conflict(request)

        # separate the options and non-option arguments
        try:
            indexes = [n for n,arg in args if arg == '--']
            idx = indexes[0]
        except Exception:
            options, nonopt_args = args[:], []
        else:
            options, nonopt_args = args[:idx], args[idx+1:]

        # the result from parsing the option, to be
        # mixed together with the non-opt arguments
        # into the final result.
        opts = {}

        # store all non-opt arguments while parse, then prepend
        # them once into 'nonopt_args', for to keep its order.
        mixed_nonopt_args = []

        # store the unrecognized options
        unrecognized = []

        # process each option in turn, 'options' may be altered
        # in self.fetch_opt_val for to get an option's argument.
        while options:

            # non-option arguments may be mixed among
            # options, so save it and move it out.
            non_opts, options = self.split_leading_non_opts(options)
            mixed_nonopt_args.extend(non_opts)

            if not options:
                break

            option = options.pop(0)
            match = self.fetch_opt_val(option, options, request, opts)
            if not match:
                if preserve:
                    # an unrecognized option may have an argument,
                    # we save the first argument that is an non-opt
                    # argument and directly behind the preserved one.
                    unrecognized.append(option)
                    non_opts, options = self.split_leading_non_opts(options)
                    if non_opts:
                        unrecognized.append(non_opts.pop(0))
                        options = non_opts + options
                else:
                    assert False, "unrecognized option %s" % option

        nonopt_args = mixed_nonopt_args + nonopt_args
        if not preserve:
            nonopt_args = [a for n,a in nonopt_args]
        result = [opts, nonopt_args, unrecognized]
        return result

    def fetch_opt_val(self, option, options, request, opts):
        """The previous parsed result have already stored in 'opts',
        new values will be added together with the olds.

        If arg_type is 0, no argument is required for the option,
        multiple options of this type can be trained together as of
        form -xy; if it is 1, argument is required, long form options
        can be specified as --option=value, or --option value, and
        short form can be -o v or -ov; if it is 2, then short form
        options may be treated as regular expressions, options like -12,
        -1 can be recognized, the value of option -12 is then set to 12,
        but to the long format, it is the same meaning as of value 1,
        the normal short format and the format like -12 are
        distinguished by the first non-dash character, when it is an
        alphabet, it is a normal short format. If the arg_type is 3,
        the argument is optional, and an equal sign must be used as the
        separator between the option and the argument if the argument
        is supplied.

        If multi is True, the value of the option will be appended to
        the associated list in the 'opts' if the value had not yet in
        the list. If it is False, then the previous value of the same
        option will be replaced.

        If order is True, the order of the option in the command line
        argument list will be preserved

        One option compares with each of the flags in the 'request'.

        Return True if option matched, otherwise False.
        """

        # the option is a tuple
        opt_pos, option = option

        for opt_name, opt_def in request.items():
            flags = opt_def['flag']
            arg_type = opt_def.get('arg', 0)
            multi = opt_def.get('multi', False)
            order = opt_def.get('order', False)
            if not isinstance(flags, (list, tuple)):
                flags = [flags]

            for flag in flags:
                val = None
                errmsg = "argument required for %s" % flag
                 # long form
                if re.match('--\w', flag):
                    if arg_type == 0:
                        if option == flag:
                            val = True
                    else:
                        heading = flag + '='
                        # --key=val
                        if option.startswith(heading):
                            val = option[len(heading):]
                        # --key val, or --key
                        elif option == flag:
                            if arg_type == 1:
                                assert len(options), errmsg
                                val = options.pop(0)[1]
                            elif arg_type == 3:
                                val = True
                # short form
                elif re.match('-[^-]+', flag):
                    if arg_type == 0:
                        if option == flag:
                            val = True
                        elif option.startswith(flag):
                            # train multiple options together
                            # add 0.001 to the position to distinguish
                            # the order of the trained options.
                            val = True
                            next_option = '-' + option[len(flag):]
                            options.insert(0, (opt_pos+0.001, next_option))
                    elif arg_type == 1:
                        if option == flag:
                            assert len(options), errmsg
                            val = options.pop(0)[1]
                        elif option.startswith(flag):
                            val = option[len(flag):]
                    elif arg_type == 2:
                        if re.match('[a-zA-Z]', option[1]):
                            # normal short format
                            if option == flag:
                                assert len(options), errmsg
                                val = options.pop(0)[1]
                            elif option.startswith(flag):
                                val = option[len(flag):]
                        elif re.match(flag, option):
                            # special short format like -12
                            val = option[1:]
                # key/value form
                elif re.match('\w+', flag):
                    heading = flag + '='
                    if option.startswith(heading):
                        val = option[len(heading):]
                else:
                    assert False, "wrong flag: %s" % flag

                # matched, option processed, return.
                if val is not None:
                    if order:
                        val = (opt_pos, val)
                    self.save_opt(opts, opt_name, val, multi)
                    return True

        return False    # not matched

    def save_opt(self, result, opt_name, val, multi):
        """Alter the 'result' in-place"""
        if multi:
            vals = result.get(opt_name, [])
            if val not in vals:
                vals.append(val)
            result[opt_name] = vals
        else:
            result[opt_name] = val

    def detect_conflict(self, request):
        """Check if there is any conflict between any
        two of the short form flags"""
        flags = []
        for v in request.values():
            flag = v['flag']
            if isinstance(flag, (list, tuple)):
                flags.extend(flag)
            else:
                flags.append(flag)
        flags = set(flags)
        flags = [x for x in flags if re.match('-\w', x)]
        if len(flags) < 2:
            return True
        # put similar ones together, shorter first
        flags.sort()
        x = flags.pop(0)
        while flags:
            y = flags.pop(0)
            assert x not in y, "%s conflict with %s" % (x, y)
            x = y

    def split_leading_non_opts(self, options):
        for idx, opt in enumerate(options):
            # 'opt' is a tuple.
            #
            # a dash followed by a non-blank character,
            # forms like if=/dev/sda, a=1, b=2,
            # are all options
            #
            # others are non-opt arguments, including
            # the special single dash '-'
            if re.match('^(-[^\s]|\w+=)', opt[1]):  # is an option
                non_opts = options[:idx]
                opts = options[idx:]
                return non_opts, opts
        return options, []  # all are non-option arguments
