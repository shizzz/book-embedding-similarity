import argparse
from app.cli.args import build_parser
from fastapi import APIRouter

router = APIRouter(prefix="/commands", tags=["commands"])

@router.get("/")
async def get_commands():
    parser = build_parser()
    return extract_commands_from_parser(parser)

import argparse

def extract_commands_from_parser(parser: argparse.ArgumentParser):
    result = {}

    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):

            for entity_name, entity_parser in action.choices.items():
                group_name = entity_name.capitalize()
                result[group_name] = []

                for sub_action in entity_parser._actions:
                    if isinstance(sub_action, argparse._SubParsersAction):

                        for command_name, command_parser in sub_action.choices.items():
                            args = {}

                            for arg in command_parser._actions:
                                if isinstance(arg, argparse._HelpAction):
                                    continue

                                if not arg.option_strings:
                                    continue  # skip positional

                                name = arg.dest

                                arg_info = {}

                                # --- default ---
                                default = arg.default
                                if default is None:
                                    if isinstance(arg, argparse._StoreTrueAction):
                                        default = False
                                    elif isinstance(arg, argparse._StoreFalseAction):
                                        default = True
                                    elif arg.nargs in ("+", "*"):
                                        default = []
                                    else:
                                        default = ""

                                arg_info["default"] = default

                                # --- type ---
                                if isinstance(arg, argparse._StoreTrueAction) or isinstance(arg, argparse._StoreFalseAction):
                                    arg_info["type"] = "bool"
                                elif arg.type in [int, float]:
                                    arg_info["type"] = arg.type.__name__
                                elif arg.nargs in ("+", "*"):
                                    arg_info["type"] = "list"
                                else:
                                    arg_info["type"] = "str"

                                # --- required ---
                                arg_info["required"] = getattr(arg, "required", False)

                                # --- choices ---
                                if arg.choices:
                                    arg_info["choices"] = list(arg.choices)

                                args[name] = arg_info

                            result[group_name].append({
                                "entity": entity_name,
                                "command": command_name,
                                "label": f"{entity_name} {command_name}",
                                "args": args
                            })

    return result